'use strict';

/**
 * Pacifica Account Lookup - remote (HTTP-hosted) MCP server.
 * Reachable over the public internet so Claude Mobile, claude.ai, and
 * Claude Desktop can all use it as a Custom Connector - not just one laptop.
 *
 * Tools:
 *  - pacifica_get_account              account balance/equity/margin/fees
 *  - pacifica_get_positions            open positions
 *  - pacifica_get_trade_history        trade history (incl. liquidation tags)
 *  - pacifica_get_mark_price_klines    mark-price candles (liquidation/SL trigger source)
 *  - pacifica_get_klines               trade-price candles (chart price, for comparison)
 *  - pacifica_get_order_history_by_id  full order config by order_id (type, stop_price, trigger_price_type, reduce_only)
 *  - pacifica_get_open_orders          currently open/pending orders (incl. untriggered stops), by account
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
