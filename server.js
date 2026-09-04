'use strict';

/**
 * Pacifica Account Lookup - remote (HTTP-hosted) MCP server.
 * Reachable over the public internet so Claude Mobile, claude.ai, and
 * Claude Desktop can all use it as a Custom Connector - not just one laptop.
 *
 * Tools:
 *  Market-wide (no account needed):
 *  - pacifica_get_account              account balance/equity/margin/fees
 *  - pacifica_get_positions            open positions
 *  - pacifica_get_trade_history        trade history (incl. liquidation tags)
 *  - pacifica_get_mark_price_klines    mark-price candles (liquidation/SL trigger source)
 *  - pacifica_get_klines               trade-price candles (chart price, for comparison)
 *  - pacifica_get_order_history_by_id  full order config by order_id (type, stop_price, trigger_price_type, reduce_only)
 *  - pacifica_get_open_orders          currently open/pending orders (incl. untriggered stops), by account
 *  - pacifica_get_market_info          per-market max_leverage/tick/lot size + funding; derive MMR = 1/(2*max_leverage)
 *  - pacifica_get_prices               live snapshot of every market at once (mark/mid/funding/OI/volume)
 *  - pacifica_get_orderbook            live bid/ask depth for a symbol
 *  - pacifica_get_recent_trades        market-wide public trade tape (not account-specific)
 *  - pacifica_get_funding_rate_history historical funding RATE for a market
 *  - pacifica_get_fees                 fee schedule/tiers
 *  - pacifica_get_loan_pool            lending pool rates/utilization (unified margin borrow/lend)
 *  - pacifica_get_vaults               list of Pacifica Vaults
 *  - pacifica_get_spot_assets          supported spot assets reference
 *
 *  Account-specific (still public by wallet address):
 *  - pacifica_get_account_funding_history  what THIS account actually paid/received in funding
 *  - pacifica_get_equity_history           account equity/PnL over time
 *  - pacifica_get_balance_history          deposit/withdrawal/payout ledger
 *  - pacifica_get_order_history            full order history list (no order_id needed up front)
 *  - pacifica_get_account_settings         margin mode, spot settings, use_ltp_for_stop_orders
 *  - pacifica_get_account_loan             borrowed amount, collateral utilization, per-asset LTV
 *  - pacifica_get_spot_balance_history     spot asset balance history
 *  - pacifica_get_spot_deposit_history     spot asset deposit history
 *  - pacifica_get_spot_withdraw_history    spot asset withdrawal history
 *  - pacifica_get_spot_withdraw_pending    currently pending spot withdrawals
 *
 * Read-only. Only calls Pacifica's public REST API. No API keys,
 * no private keys, nothing sensitive stored or required.
 */

const express = require('express');
const https = require('https');
const { z } = require('zod');
const { McpServer } = require('@modelcontextprotocol/sdk/server/mcp.js');
const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');

const BASE_URL = 'https://api.pacifica.fi/api/v1';

function fetchJson(path) {
  return new Promise((resolve, reject) => {
    https
      .get(BASE_URL + path, { headers: { Accept: 'application/json' } }, (res) => {
        let data = '';
        res.on('data', (chunk) => (data += chunk));
        res.on('end', () => {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(new Error('Pacifica API returned unexpected data: ' + data.slice(0, 300)));
          }
        });
      })
      .on('error', reject);
  });
}

// Accepts either a millisecond epoch (number or numeric string) or an
// ISO-8601 date string like "2026-08-12T12:30:00Z" and returns ms epoch.
function parseTimeToMs(value) {
  if (value === undefined || value === null || value === '') return undefined;
  if (typeof value === 'number') return Math.trunc(value);
  const trimmed = String(value).trim();
  if (/^\d+$/.test(trimmed)) return parseInt(trimmed, 10);
  const parsed = Date.parse(trimmed);
  if (Number.isNaN(parsed)) {
    throw new Error(`Could not parse time value "${value}". Use a millisecond epoch or an ISO-8601 string (e.g. "2026-08-12T12:30:00Z").`);
  }
  return parsed;
}

const KLINE_INTERVALS = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '12h', '1d'];

function buildServer() {
  const server = new McpServer({ name: 'pacifica-account-lookup', version: '1.0.0' });

  server.registerTool(
    'pacifica_get_account',
    {
      title: 'Get Pacifica account info',
      description:
        'Get balance, equity, fee level, and margin info for ANY Pacifica account by wallet address (not just one fixed account). Use this first when investigating a support ticket about a specific customer wallet.',
      inputSchema: { address: z.string().describe('The Solana wallet address of the Pacifica account') }
    },
    async ({ address }) => {
      try {
        const data = await fetchJson(`/account?account=${encodeURIComponent(address)}`);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_positions',
    {
      title: 'Get Pacifica positions',
      description: 'Get current open positions for any Pacifica account by wallet address.',
      inputSchema: { address: z.string().describe('The Solana wallet address of the Pacifica account') }
    },
    async ({ address }) => {
      try {
        const data = await fetchJson(`/positions?account=${encodeURIComponent(address)}`);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_trade_history',
    {
      title: 'Get Pacifica trade history',
      description:
        'Get recent trade history for any Pacifica account by wallet address. Each trade is tagged normal, market_liquidation, backstop_liquidation, or settlement - use this to investigate liquidation tickets.',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account'),
        limit: z.number().optional().describe('Max number of trades to return (default 20)')
      }
    },
    async ({ address, limit }) => {
      try {
        const data = await fetchJson(`/trades/history?account=${encodeURIComponent(address)}&limit=${limit || 20}`);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_mark_price_klines',
    {
      title: 'Get Pacifica mark price candles',
      description:
        'Get historical MARK PRICE candles for a market and time window (1-minute granularity available). Mark price - not last-traded price - is what actually triggers liquidations and stop-loss/take-profit orders on Pacifica, so this is the authoritative source for "did the mark price really reach this level at this time" investigations. Use this to independently verify a suspected early/late SL or liquidation trigger. Times may be given as millisecond epochs or ISO-8601 strings (e.g. "2026-08-12T12:30:00Z").',
      inputSchema: {
        symbol: z.string().describe('Market symbol, e.g. BTC, EURUSD'),
        interval: z.enum(KLINE_INTERVALS).describe('Candle interval. Use 1m for precise trigger-time investigation.'),
        start_time: z.union([z.number(), z.string()]).describe('Start of window: ms epoch or ISO-8601 string'),
        end_time: z.union([z.number(), z.string()]).optional().describe('End of window: ms epoch or ISO-8601 string (defaults to now)')
      }
    },
    async ({ symbol, interval, start_time, end_time }) => {
      try {
        const startMs = parseTimeToMs(start_time);
        const endMs = parseTimeToMs(end_time);
        let path = `/kline/mark?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&start_time=${startMs}`;
        if (endMs !== undefined) path += `&end_time=${endMs}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_klines',
    {
      title: 'Get Pacifica trade-price candles',
      description:
        'Get historical candles based on LAST TRADED PRICE (not mark price) for a market and time window. Useful for comparing against pacifica_get_mark_price_klines to explain discrepancies between what the chart showed and what actually triggered a liquidation/SL - chart candles are trade price, triggers use mark price. Times may be given as millisecond epochs or ISO-8601 strings.',
      inputSchema: {
        symbol: z.string().describe('Market symbol, e.g. BTC, EURUSD'),
        interval: z.enum(KLINE_INTERVALS).describe('Candle interval. Use 1m for precise trigger-time investigation.'),
        start_time: z.union([z.number(), z.string()]).describe('Start of window: ms epoch or ISO-8601 string'),
        end_time: z.union([z.number(), z.string()]).optional().describe('End of window: ms epoch or ISO-8601 string (defaults to now)')
      }
    },
    async ({ symbol, interval, start_time, end_time }) => {
      try {
        const startMs = parseTimeToMs(start_time);
        const endMs = parseTimeToMs(end_time);
        let path = `/kline?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&start_time=${startMs}`;
        if (endMs !== undefined) path += `&end_time=${endMs}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_order_history_by_id',
    {
      title: 'Get Pacifica order history by order ID',
      description:
        'Get the full lifecycle/configuration of a specific order by its order_id - including order_type (e.g. stop_loss_market, stop_loss_limit, take_profit_market, take_profit_limit, limit, market), stop_price, reduce_only, and trigger_price_type (mark_price, last_trade_price, or mid_price - this tells you what the order was ACTUALLY configured to trigger against). Use this after pacifica_get_trade_history to look up the order_id of a closed/filled trade and verify exactly how its SL/TP was configured - this is the only way to confirm trigger price and trigger price type, which trade history does not expose.',
      inputSchema: {
        order_id: z.union([z.number(), z.string()]).describe('The order_id to look up (found in pacifica_get_trade_history results)')
      }
    },
    async ({ order_id }) => {
      try {
        const data = await fetchJson(`/orders/history_by_id?order_id=${encodeURIComponent(order_id)}`);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_open_orders',
    {
      title: 'Get Pacifica open orders',
      description:
        'Get all currently open/pending orders (including resting stop/TP/SL orders that have not yet triggered) for an account. Useful for tickets about an order that "should have filled" or "disappeared" - shows whether it is still resting, and its configured type/stop_price/trigger_price_type. For ALREADY-closed/filled/cancelled orders use pacifica_get_order_history_by_id instead.',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account')
      }
    },
    async ({ address }) => {
      try {
        const data = await fetchJson(`/orders?account=${encodeURIComponent(address)}`);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_market_info',
    {
      title: 'Get Pacifica market info (leverage, tick/lot size, MMR)',
      description:
        'Get per-market trading parameters for every Pacifica market: max_leverage, tick_size, lot_size, min/max order size, and current/next funding rate. Critically, this is what you need to independently verify liquidation math: on Pacifica, maintenance margin is ALWAYS exactly half the initial margin, and initial margin = 1 / max_leverage. So the maintenance margin ratio (MMR) for any market = 1 / (2 * max_leverage) - e.g. max_leverage 50 -> MMR = 1%. Combine with mark price (pacifica_get_mark_price_klines) and Pacifica\'s published formula liquidation_price = [price - (side * position_margin) / position_size] / (1 - side / max_leverage / 2) (side = 1 for long, -1 for short; position_margin = account equity for cross-margin) to check a liquidation price precisely instead of guessing at an MMR number.',
      inputSchema: {}
    },
    async () => {
      try {
        const data = await fetchJson('/info');
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  // ---- Market-wide tools ----

  server.registerTool(
    'pacifica_get_prices',
    {
      title: 'Get Pacifica live prices (all markets)',
      description:
        'Get a live snapshot of every Pacifica market at once: mark price, mid price, funding rate, next funding rate, open interest, oracle price, 24h volume, and yesterday\'s price. One call instead of querying symbols individually - useful for quick "what is X trading at right now" or scanning for unusual moves across markets.',
      inputSchema: {}
    },
    async () => {
      try {
        const data = await fetchJson('/info/prices');
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_orderbook',
    {
      title: 'Get Pacifica order book',
      description:
        'Get the current order book (bid/ask levels, up to 10 per side) for a market symbol. Use this to independently verify liquidity/slippage claims - e.g. whether a thin book explains a worse-than-expected fill price, rather than assuming.',
      inputSchema: {
        symbol: z.string().describe('Market symbol, e.g. BTC'),
        agg_level: z.number().int().optional().describe('Aggregation level for price grouping. Defaults to 1.')
      }
    },
    async ({ symbol, agg_level }) => {
      try {
        let path = `/book?symbol=${encodeURIComponent(symbol)}`;
        if (agg_level !== undefined) path += `&agg_level=${agg_level}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_recent_trades',
    {
      title: 'Get Pacifica market-wide recent trades',
      description:
        'Get the most recent public trades for a market symbol - the venue-wide trade tape, NOT one account\'s trades (use pacifica_get_trade_history for a specific account). Useful for corroborating whether a claimed fill price was actually possible on the market at that time.',
      inputSchema: {
        symbol: z.string().describe('Market symbol to query trades for.'),
        limit: z.number().int().min(1).optional().describe('Number of trades to return (default system-defined).')
      }
    },
    async ({ symbol, limit }) => {
      try {
        let path = `/trades?symbol=${encodeURIComponent(symbol)}`;
        if (limit) path += `&limit=${limit}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_funding_rate_history',
    {
      title: 'Get Pacifica market funding rate history',
      description:
        'Get the historical funding RATE for a market (oracle price, bid/ask impact price, funding_rate, next_funding_rate per hourly epoch). This is the market-level rate, not what any specific account paid - use pacifica_get_account_funding_history for that.',
      inputSchema: {
        symbol: z.string().describe('Market symbol to query.'),
        limit: z.number().int().min(1).max(4000).optional().describe('Number of records (default 100, max 4000).'),
        cursor: z.string().optional().describe('Cursor for pagination.')
      }
    },
    async ({ symbol, limit, cursor }) => {
      try {
        let path = `/funding_rate/history?symbol=${encodeURIComponent(symbol)}`;
        if (limit) path += `&limit=${limit}`;
        if (cursor) path += `&cursor=${encodeURIComponent(cursor)}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_fees',
    {
      title: 'Get Pacifica fee schedule',
      description: 'Get the current trading fee schedule/tiers. Use this to check fee disputes against actual published rates instead of memory.',
      inputSchema: {}
    },
    async () => {
      try {
        const data = await fetchJson('/info/fees');
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_loan_pool',
    {
      title: 'Get Pacifica loan pool info',
      description: 'Get lending pool rates and utilization for Pacifica\'s unified-margin borrow/lend feature (market-level, not one account\'s loan). Use pacifica_get_account_loan for a specific account\'s borrow/collateral position.',
      inputSchema: {}
    },
    async () => {
      try {
        const data = await fetchJson('/loan_pool');
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_vaults',
    {
      title: 'Get Pacifica Vaults',
      description: 'List Pacifica Vaults (user-deployed managed vaults) - names, managers, and stats. Useful for tickets about vault deposits, performance, or manager verification.',
      inputSchema: {}
    },
    async () => {
      try {
        const data = await fetchJson('/lake/list');
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_spot_assets',
    {
      title: 'Get Pacifica spot assets',
      description: 'List spot assets supported on Pacifica as reference/collateral. Useful for "is X asset supported" and deposit-related questions.',
      inputSchema: {}
    },
    async () => {
      try {
        const data = await fetchJson('/spot_assets');
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  // ---- Account-specific tools (still public by wallet address) ----

  server.registerTool(
    'pacifica_get_account_funding_history',
    {
      title: 'Get Pacifica account funding history',
      description:
        'Get what a SPECIFIC account actually paid or received in funding over time (not the market rate - use pacifica_get_funding_rate_history for that). Directly settles "I was charged/paid funding I did not expect" disputes.',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account'),
        limit: z.number().int().min(1).optional().describe('Max records to return (default system-defined).'),
        cursor: z.string().optional().describe('Cursor for pagination.')
      }
    },
    async ({ address, limit, cursor }) => {
      try {
        let path = `/funding/history?account=${encodeURIComponent(address)}`;
        if (limit) path += `&limit=${limit}`;
        if (cursor) path += `&cursor=${encodeURIComponent(cursor)}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_equity_history',
    {
      title: 'Get Pacifica account equity/PnL history',
      description: 'Get account equity and PnL history over time for a specific account. Directly settles "my balance/equity does not match what I expect" disputes.',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account'),
        start_time: z.union([z.number(), z.string()]).optional().describe('Start time: ms epoch or ISO-8601 string.'),
        end_time: z.union([z.number(), z.string()]).optional().describe('End time: ms epoch or ISO-8601 string.')
      }
    },
    async ({ address, start_time, end_time }) => {
      try {
        let path = `/portfolio?account=${encodeURIComponent(address)}`;
        if (start_time !== undefined) path += `&start_time=${parseTimeToMs(start_time)}`;
        if (end_time !== undefined) path += `&end_time=${parseTimeToMs(end_time)}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_balance_history',
    {
      title: 'Get Pacifica account balance history',
      description:
        'Get all balance-affecting events for an account: deposits, withdrawals, payouts (e.g. affiliate program), etc. This is the ledger for "where is my deposit/withdrawal" tickets - one of the most common support categories.',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account'),
        limit: z.number().int().min(1).optional().describe('Max records to return (default system-defined).'),
        cursor: z.string().optional().describe('Cursor for pagination.')
      }
    },
    async ({ address, limit, cursor }) => {
      try {
        let path = `/account/balance/history?account=${encodeURIComponent(address)}`;
        if (limit) path += `&limit=${limit}`;
        if (cursor) path += `&cursor=${encodeURIComponent(cursor)}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_order_history',
    {
      title: 'Get Pacifica account order history (list)',
      description:
        'Get an account\'s full order history as a list - use this to browse without needing an order_id up front. For full config detail (type, stop_price, trigger_price_type) on one specific order, chain into pacifica_get_order_history_by_id using the order_id from these results.',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account'),
        limit: z.number().int().min(1).optional().describe('Max records to return (default system-defined).'),
        cursor: z.string().optional().describe('Cursor for pagination.')
      }
    },
    async ({ address, limit, cursor }) => {
      try {
        let path = `/orders/history?account=${encodeURIComponent(address)}`;
        if (limit) path += `&limit=${limit}`;
        if (cursor) path += `&cursor=${encodeURIComponent(cursor)}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_account_settings',
    {
      title: 'Get Pacifica account settings',
      description:
        'Get an account\'s configured settings: margin_settings, spot_settings, auto_lend_disabled, and use_ltp_for_stop_orders. That last field explains "why did my stop trigger off a different price than I expected" - it controls whether stops use last-traded price instead of the mark-price default.',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account')
      }
    },
    async ({ address }) => {
      try {
        const data = await fetchJson(`/account/settings?account=${encodeURIComponent(address)}`);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_account_loan',
    {
      title: 'Get Pacifica account loan/collateral detail',
      description:
        'Get a specific account\'s borrow/collateral detail under unified margin: borrowed amount, collateral utilization, and per-asset LTV. Relevant for margin/liquidation disputes that involve spot collateral rather than pure USDC margin.',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account')
      }
    },
    async ({ address }) => {
      try {
        const data = await fetchJson(`/account/loan?account=${encodeURIComponent(address)}`);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_spot_balance_history',
    {
      title: 'Get Pacifica spot balance history',
      description: 'Get spot asset balance history for an account (spot-asset equivalent of pacifica_get_balance_history, which covers USDC/margin balance).',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account'),
        limit: z.number().int().min(1).optional().describe('Max records to return (default system-defined).'),
        cursor: z.string().optional().describe('Cursor for pagination.')
      }
    },
    async ({ address, limit, cursor }) => {
      try {
        let path = `/account/spot_balance/history?account=${encodeURIComponent(address)}`;
        if (limit) path += `&limit=${limit}`;
        if (cursor) path += `&cursor=${encodeURIComponent(cursor)}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_spot_deposit_history',
    {
      title: 'Get Pacifica spot deposit history',
      description: 'Get an account\'s spot asset deposit history. Use for "where is my deposit" tickets involving spot assets specifically.',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account'),
        limit: z.number().int().min(1).optional().describe('Max records to return (default system-defined).'),
        cursor: z.string().optional().describe('Cursor for pagination.')
      }
    },
    async ({ address, limit, cursor }) => {
      try {
        let path = `/account/spot_asset/deposit/history?account=${encodeURIComponent(address)}`;
        if (limit) path += `&limit=${limit}`;
        if (cursor) path += `&cursor=${encodeURIComponent(cursor)}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_spot_withdraw_history',
    {
      title: 'Get Pacifica spot withdrawal history',
      description: 'Get an account\'s completed spot asset withdrawal history. For currently-pending withdrawals, use pacifica_get_spot_withdraw_pending instead.',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account'),
        limit: z.number().int().min(1).optional().describe('Max records to return (default system-defined).'),
        cursor: z.string().optional().describe('Cursor for pagination.')
      }
    },
    async ({ address, limit, cursor }) => {
      try {
        let path = `/account/spot_asset/withdraw/history?account=${encodeURIComponent(address)}`;
        if (limit) path += `&limit=${limit}`;
        if (cursor) path += `&cursor=${encodeURIComponent(cursor)}`;
        const data = await fetchJson(path);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  server.registerTool(
    'pacifica_get_spot_withdraw_pending',
    {
      title: 'Get Pacifica pending spot withdrawals',
      description:
        'Get an account\'s currently PENDING spot asset withdrawals - the direct answer for "my withdrawal has been stuck for X" tickets: is it still processing, and since when.',
      inputSchema: {
        address: z.string().describe('The Solana wallet address of the Pacifica account')
      }
    },
    async ({ address }) => {
      try {
        const data = await fetchJson(`/account/spot_asset/withdraw/pending?account=${encodeURIComponent(address)}`);
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text', text: 'Error: ' + err.message }], isError: true };
      }
    }
  );

  return server;
}

const app = express();
app.use(express.json());

// Stateless MCP endpoint: a fresh server+transport per request, no session state kept.
app.post('/mcp', async (req, res) => {
  try {
    const server = buildServer();
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    res.on('close', () => {
      transport.close();
      server.close();
    });
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    console.error('MCP request error:', err);
    if (!res.headersSent) {
      res.status(500).json({ jsonrpc: '2.0', error: { code: -32603, message: 'Internal server error' }, id: null });
    }
  }
});

app.get('/mcp', (req, res) => {
  res
    .status(405)
    .json({ jsonrpc: '2.0', error: { code: -32000, message: 'This server is stateless; only POST is supported.' }, id: null });
});
app.delete('/mcp', (req, res) => res.status(405).end());

app.get('/', (req, res) => {
  res.send('Pacifica Account Lookup MCP server is running. Point your MCP client at /mcp');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Pacifica lookup MCP server listening on port ${PORT}`);
});
