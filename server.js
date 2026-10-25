'use strict';

/**
 * Pacifica Lookup - remote (HTTP-hosted) MCP server, v2.
 * Reachable over the public internet so Claude Mobile, claude.ai, and
 * Claude Desktop can all use it as a Custom Connector.
 *
 * READ-ONLY. Only calls Pacifica's public, unsigned GET endpoints (plus the
 * public docs and status pages). No private keys, no signing, no account
 * secrets. Anything that needs a signature (e.g. listing subaccounts) is
 * deliberately out of scope.
 *
 * Endpoint/parameter names were verified against Pacifica's official docs
 * (docs.pacifica.fi) and the official @pacifica-fi/mcp-server source.
 *
 * Tools - markets:
 *   pacifica_get_market_info, pacifica_get_prices, pacifica_get_orderbook,
 *   pacifica_get_recent_trades, pacifica_get_klines, pacifica_get_mark_price_klines,
 *   pacifica_get_funding_rate_history, pacifica_get_aggregated_funding_rates,
 *   pacifica_get_fees, pacifica_get_loan_pool, pacifica_get_vaults,
 *   pacifica_get_spot_assets, pacifica_get_bridge_info
 * Tools - account (public by wallet address, main account or subaccount):
 *   pacifica_get_account, pacifica_get_positions, pacifica_get_position_liquidation_prices,
 *   pacifica_get_account_settings, pacifica_get_account_loan,
 *   pacifica_get_equity_history, pacifica_get_balance_history (full ledger via include_trades),
 *   pacifica_get_trade_history, pacifica_get_account_funding_history,
 *   pacifica_get_deposit_history, pacifica_get_withdrawal_history,
 *   pacifica_get_pending_withdrawals, pacifica_get_payout_history
 * Tools - orders:
 *   pacifica_get_open_orders, pacifica_get_order_history,
 *   pacifica_get_order_history_by_id, pacifica_get_twap_orders
 * Tools - spot:
 *   pacifica_get_spot_balance_history, pacifica_get_spot_deposit_history,
 *   pacifica_get_spot_withdraw_history, pacifica_get_spot_withdraw_pending
 * Tools - reference/utility:
 *   pacifica_search_docs, pacifica_read_doc (cite-able official docs),
 *   pacifica_get_status (status.pacifica.fi), pacifica_convert_time
 *
 * Optional environment variables (all have safe defaults):
 *   PACIFICA_API_KEY   - sent as the PF-API-KEY header (rate-limit tier only)
 *   PACIFICA_API_BASE, DOCS_BASE_URL, STATUS_BASE_URL - override hosts (testing)
 */

const express = require('express');
const https = require('https');
const http = require('http');
const { z } = require('zod');
const { McpServer } = require('@modelcontextprotocol/sdk/server/mcp.js');
const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');

const BASE_URL = (process.env.PACIFICA_API_BASE || 'https://api.pacifica.fi/api/v1').replace(/\/$/, '');
const DOCS_BASE = (process.env.DOCS_BASE_URL || 'https://docs.pacifica.fi').replace(/\/$/, '');
const STATUS_BASE = (process.env.STATUS_BASE_URL || 'https://status.pacifica.fi').replace(/\/$/, '');
const PUBLIC_DOCS = 'https://docs.pacifica.fi';
const PUBLIC_STATUS = 'https://status.pacifica.fi';
const API_KEY = process.env.PACIFICA_API_KEY || '';

/* ------------------------------------------------------------------ */
/* HTTP plumbing                                                       */
/* ------------------------------------------------------------------ */

function httpGet(url, { headers = {}, timeoutMs = 20000, redirects = 3 } = {}) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https:') ? https : http;
    const req = lib.get(url, { headers: { 'User-Agent': 'pacifica-lookup-mcp/2.0', ...headers } }, (res) => {
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location && redirects > 0) {
        res.resume();
        const next = new URL(res.headers.location, url).toString();
        return resolve(httpGet(next, { headers, timeoutMs, redirects: redirects - 1 }));
      }
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (c) => (body += c));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body }));
    });
    req.setTimeout(timeoutMs, () => req.destroy(new Error(`Request timed out after ${timeoutMs / 1000}s`)));
    req.on('error', reject);
  });
}

function buildQuery(q = {}) {
  const parts = [];
  for (const [k, v] of Object.entries(q)) {
    if (v === undefined || v === null || v === '') continue;
    parts.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
  }
  return parts.length ? '?' + parts.join('&') : '';
}

// Pacifica reports credits x10:  ratelimit: "credits";r=1200;t=32  =>  120.0 credits left, refresh in 32s
function parseRateLimit(headers) {
  const h = headers && headers['ratelimit'];
  if (!h) return null;
  const m = /r=(\d+)\s*;\s*t=(\d+)/.exec(String(h));
  if (!m) return null;
  return { remaining_credits: Number(m[1]) / 10, resets_in_seconds: Number(m[2]) };
}

class ApiError extends Error {
  constructor(message, status, rate) {
    super(message);
    this.status = status;
    this.rate = rate || null;
  }
}

async function apiGet(path, query) {
  const headers = { Accept: 'application/json' };
  if (API_KEY) headers['PF-API-KEY'] = API_KEY;
  const res = await httpGet(BASE_URL + path + buildQuery(query), { headers });
  const rate = parseRateLimit(res.headers);
  if (res.status === 429) throw new ApiError('rate limited', 429, rate);
  let json;
  try {
    json = JSON.parse(res.body);
  } catch (e) {
    throw new ApiError(`Pacifica API returned HTTP ${res.status} with unexpected (non-JSON) data: ${String(res.body).slice(0, 300)}`, res.status, rate);
  }
  return { status: res.status, json, rate };
}

function rateLimitMessage(rate) {
  const wait = rate ? `Credits refill in about ${rate.resets_in_seconds}s.` : 'Wait about a minute.';
  return (
    'Pacifica rate limit hit (HTTP 429). This connector shares one 100-credit-per-60-seconds budget across everyone using it. ' +
    wait +
    ' Then retry - and prefer a larger "limit" over many pages.'
  );
}

/**
 * Fetch one page (returned raw, unchanged) or several pages merged together.
 * Multi-page mode follows next_cursor, optionally filters rows by time locally
 * (for endpoints with no server-side date filter), and degrades gracefully if
 * the shared rate-limit budget runs low.
 */
async function apiGetPaged(path, query, { maxPages = 1, startMs, endMs, timeField = 'created_at' } = {}) {
  const hasFilter = startMs !== undefined || endMs !== undefined;
  if (maxPages <= 1 && !hasFilter) {
    return (await apiGet(path, query)).json;
  }

  const rows = [];
  let cursor = query.cursor;
  let pages = 0;
  let scanned = 0;
  let lastJson = null;
  let rate = null;
  let stop = null;

  while (pages < maxPages) {
    let r;
    try {
      r = await apiGet(path, { ...query, cursor });
    } catch (e) {
      if (e.status === 429 && pages > 0) {
        stop = 'rate_limited';
        rate = e.rate || rate;
        break;
      }
      throw e;
    }
    rate = r.rate || rate;
    if (!r.json || r.json.success === false || !Array.isArray(r.json.data)) {
      if (pages === 0) return r.json; // error or non-list payload: hand it back untouched
      stop = 'unexpected_response';
      break;
    }
    pages++;
    lastJson = r.json;
    const data = r.json.data;
    scanned += data.length;

    for (const row of data) {
      const t = row && typeof row[timeField] === 'number' ? row[timeField] : undefined;
      if (t !== undefined) {
        if (startMs !== undefined && t < startMs) continue;
        if (endMs !== undefined && t > endMs) continue;
      }
      rows.push(row);
    }

    if (hasFilter) {
      const ts = data.map((row) => row && row[timeField]).filter((x) => typeof x === 'number');
      if (ts.length >= 2) {
        const descending = ts[0] >= ts[ts.length - 1];
        if (descending && startMs !== undefined && Math.min(...ts) < startMs) {
          stop = 'reached_start_time';
          break;
        }
        if (!descending && endMs !== undefined && Math.max(...ts) > endMs) {
          stop = 'passed_end_time';
          break;
        }
      }
    }

    if (!r.json.has_more || !r.json.next_cursor) {
      cursor = null;
      break;
    }
    cursor = r.json.next_cursor;
    if (rate && rate.remaining_credits < 20 && pages < maxPages) {
      stop = 'low_rate_limit_credits';
      break;
    }
  }

  const moreAvailable = !!(lastJson && lastJson.has_more && lastJson.next_cursor);
  const dateStop = stop === 'reached_start_time' || stop === 'passed_end_time';
  const out = {
    success: true,
    data: rows,
    pages_fetched: pages,
    rows_scanned: scanned,
    stopped_because: stop || (moreAvailable ? 'max_pages_reached' : 'end_of_data')
  };
  if (hasFilter) {
    out.date_filter = {
      start_time_ms: startMs === undefined ? null : startMs,
      end_time_ms: endMs === undefined ? null : endMs,
      note: 'Applied locally after fetching - this endpoint has no server-side date filter.'
    };
  }
  if (moreAvailable && !dateStop) {
    out.has_more = true;
    out.next_cursor = lastJson.next_cursor;
  }
  if (rate) out.rate_limit = rate;
  return out;
}

const ok = (obj) => ({ content: [{ type: 'text', text: typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2) }] });
const fail = (msg) => ({ content: [{ type: 'text', text: 'Error: ' + msg }], isError: true });

/* ------------------------------------------------------------------ */
/* Time helpers                                                        */
/* ------------------------------------------------------------------ */

// Accepts a millisecond epoch (number or numeric string) or an ISO-8601 string.
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

/* ------------------------------------------------------------------ */
/* Docs index (llms.txt + per-page markdown), cached in memory         */
/* ------------------------------------------------------------------ */

const DOCS_TTL_MS = 6 * 60 * 60 * 1000;
let docsIndex = null;
let docsBuilding = null;

function parseLlmsTxt(text) {
  const entries = [];
  const re = /^- \[([^\]]+)\]\((https?:\/\/[^)\s]+)\)(?::\s*(.*))?$/gm;
  let m;
  while ((m = re.exec(text))) {
    let pathname;
    try {
      pathname = new URL(m[2]).pathname;
    } catch (e) {
      continue;
    }
    entries.push({ title: m[1].replace(/\\_/g, '_').trim(), path: pathname.replace(/\.md$/, '').replace(/\/$/, ''), desc: (m[3] || '').trim() });
  }
  return entries;
}

function cleanDoc(md) {
  return String(md)
    .replace(/^(?:>[^\n]*\n)+\s*/, '')
    .replace(/\s*This documentation is built and hosted on \[Mintlify\][^\n]*\s*$/i, '')
    .trim();
}

function toPlain(md) {
  return String(md)
    .replace(/<[^>]+>/g, ' ')
    .replace(/[`*_#>|]/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/\s+/g, ' ')
    .trim();
}

async function mapPool(items, concurrency, fn) {
  const out = new Array(items.length).fill(null);
  let next = 0;
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    for (;;) {
      const i = next++;
      if (i >= items.length) return;
      try {
        out[i] = await fn(items[i], i);
      } catch (e) {
        out[i] = null;
      }
    }
  });
  await Promise.all(workers);
  return out;
}

async function buildDocsIndex() {
  const idx = await httpGet(`${DOCS_BASE}/llms.txt`);
  if (idx.status !== 200) throw new Error(`Could not load the docs index (HTTP ${idx.status}).`);
  const entries = parseLlmsTxt(idx.body);
  if (!entries.length) throw new Error('The docs index was empty or in an unexpected format.');
  const texts = await mapPool(entries, 6, async (e) => {
    const r = await httpGet(`${DOCS_BASE}${e.path}.md`);
    return r.status === 200 ? cleanDoc(r.body) : null;
  });
  const pages = [];
  entries.forEach((e, i) => {
    if (!texts[i]) return;
    pages.push({ ...e, url: PUBLIC_DOCS + e.path, md: texts[i], plain: toPlain(texts[i]) });
  });
  if (!pages.length) throw new Error('Could not load any documentation pages.');
  return { builtAt: Date.now(), pages, byPath: new Map(pages.map((p) => [p.path, p])), listed: entries.length };
}

async function getDocsIndex() {
  const fresh = docsIndex && Date.now() - docsIndex.builtAt < DOCS_TTL_MS;
  if (!fresh && !docsBuilding) {
    docsBuilding = buildDocsIndex()
      .then((i) => {
        docsIndex = i;
        return i;
      })
      .finally(() => {
        docsBuilding = null;
      });
    docsBuilding.catch(() => {});
  }
  if (docsIndex) return docsIndex; // serve (possibly stale) immediately; refresh happens in background
  const building = docsBuilding;
  const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('DOCS_WARMING')), 30000));
  try {
    return await Promise.race([building, timeout]);
  } catch (e) {
    if (e.message === 'DOCS_WARMING') {
      throw new Error('The docs index is still warming up (the first load takes ~15-30s after the server wakes). Retry in about 20 seconds.');
    }
    throw e;
  }
}

const STOP_WORDS = new Set(['the', 'an', 'and', 'or', 'of', 'to', 'in', 'on', 'for', 'is', 'are', 'was', 'were', 'be', 'my', 'it', 'this', 'that', 'how', 'why', 'what', 'do', 'does', 'did', 'can', 'with', 'as', 'at', 'by', 'from', 'not', 'me', 'we', 'you']);

function stem(t) {
  if (t.length <= 3) return t;
  if (t.length === 4) return t.endsWith('s') ? t.slice(0, 3) : t;
  return t.slice(0, Math.max(5, t.length - 3));
}

function tokenize(q) {
  const toks = (String(q).toLowerCase().match(/[a-z0-9_]+/g) || []).filter((t) => t.length > 1 && !STOP_WORDS.has(t));
  return Array.from(new Set(toks.map(stem)));
}

function countOccurrences(hay, needle, cap) {
  let c = 0;
  let from = 0;
  while (c < cap) {
    const i = hay.indexOf(needle, from);
    if (i === -1) break;
    c++;
    from = i + needle.length;
  }
  return c;
}

function scorePage(p, stems, phrase) {
  const title = p.title.toLowerCase();
  const path = p.path.toLowerCase();
  const desc = p.desc.toLowerCase();
  const text = p.plain.toLowerCase();
  let s = 0;
  for (const t of stems) {
    if (title.includes(t)) s += 10;
    if (path.includes(t)) s += 4;
    if (desc.includes(t)) s += 5;
    s += countOccurrences(text, t, 10);
  }
  if (phrase.length > 3) {
    if (title.includes(phrase)) s += 15;
    if (text.includes(phrase)) s += 8;
  }
  return s;
}

function bestSnippet(plain, stems, width = 500) {
  const lower = plain.toLowerCase();
  const positions = [];
  for (const st of stems) {
    let from = 0;
    for (let n = 0; n < 40; n++) {
      const i = lower.indexOf(st, from);
      if (i === -1) break;
      positions.push(i);
      from = i + st.length;
    }
  }
  if (!positions.length) return plain.slice(0, width) + (plain.length > width ? '...' : '');
  positions.sort((a, b) => a - b);
  let bestStart = Math.max(0, positions[0] - 100);
  let bestCount = 0;
  for (const p of positions) {
    const start = Math.max(0, p - 100);
    const count = positions.filter((q) => q >= start && q < start + width).length;
    if (count > bestCount) {
      bestCount = count;
      bestStart = start;
    }
  }
  const end = Math.min(plain.length, bestStart + width);
  return (bestStart > 0 ? '...' : '') + plain.slice(bestStart, end) + (end < plain.length ? '...' : '');
}

/* ------------------------------------------------------------------ */
/* Status page                                                         */
/* ------------------------------------------------------------------ */

function htmlToText(html) {
  return String(html)
    .replace(/<(script|style|noscript|svg)[\s\S]*?<\/\1>/gi, ' ')
    .replace(/<(br|\/p|\/div|\/li|\/h[1-6]|\/tr)\s*\/?>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/[ \t]+/g, ' ')
    .replace(/\n\s*\n+/g, '\n')
    .trim();
}

async function fetchStatus(section) {
  const paths = { current: '/', incidents: '/incidents', maintenance: '/maintenance' };
  if (section === 'current') {
    try {
      const r = await httpGet(`${STATUS_BASE}/index.json`, { headers: { Accept: 'application/json' } });
      if (r.status === 200) {
        const parsed = JSON.parse(r.body);
        let text = JSON.stringify(parsed, null, 2);
        let truncated = false;
        if (text.length > 15000) {
          text = text.slice(0, 15000);
          truncated = true;
        }
        return { source: `${PUBLIC_STATUS}/index.json`, format: 'json', truncated, content: truncated ? text : parsed };
      }
    } catch (e) {
      /* fall through to the HTML page */
    }
  }
  const r = await httpGet(`${STATUS_BASE}${paths[section]}`);
  if (r.status !== 200) throw new Error(`The status page returned HTTP ${r.status}.`);
  let text = htmlToText(r.body);
  const truncated = text.length > 8000;
  if (truncated) text = text.slice(0, 8000);
  return {
    source: PUBLIC_STATUS + paths[section],
    format: 'text',
    truncated,
    note: 'Plain text extracted from the public status page; verify at the source link.',
    content: text
  };
}

/* ------------------------------------------------------------------ */
/* Tool definitions                                                    */
/* ------------------------------------------------------------------ */

const KLINE_INTERVALS = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '12h', '1d'];

const addr = (required = true) => {
  const s = z.string().describe('Solana wallet address of the Pacifica account - a main account OR a subaccount address (both work)');
  return required ? s : z.string().optional().describe('Solana wallet address of the Pacifica account (main account or subaccount)');
};
const timeArg = (what) => z.union([z.number(), z.string()]).optional().describe(`${what}. ms epoch or ISO-8601 string such as 2026-08-12T12:30:00Z`);
const cursorArg = z.string().optional().describe("Pagination cursor: pass the previous response's next_cursor to get the next page");
const limitArg = (max, note = '') =>
  (max ? z.number().int().min(1).max(max) : z.number().int().min(1)).optional().describe(`Max records per request${max ? ` (1-${max})` : ''}${note}. A larger limit is cheaper than many pages`);
const maxPagesArg = z
  .number()
  .int()
  .min(1)
  .max(5)
  .optional()
  .describe('Auto-follow next_cursor for up to this many pages and return the rows combined (default 1, max 5). Pages use rate-limit credits shared by everyone using this connector, so prefer a larger limit');

const PAGING_NOTE =
  ' The response includes next_cursor and has_more: if has_more is true there are more records - pass next_cursor as cursor, or set max_pages.';
const LOCAL_FILTER_NOTE = ' start_time/end_time filter rows locally (the API has no date filter here) and auto-page up to max_pages (default 3).';

function registerTools(server) {
  const def = ({ name, title, description, shape = {}, run }) => {
    server.registerTool(name, { title, description, inputSchema: shape }, async (args) => {
      try {
        return ok(await run(args || {}));
      } catch (err) {
        if (err instanceof ApiError && err.status === 429) return fail(rateLimitMessage(err.rate));
        return fail(err.message);
      }
    });
  };

  // Simple pass-through GET.
  const simple = (name, title, description, path, shape, toQuery) =>
    def({
      name,
      title,
      description,
      shape,
      run: async (a) => (await apiGet(path, toQuery ? toQuery(a) : {})).json
    });

  // Paged GET. localFilter=true => start_time/end_time are applied locally on `created_at`.
  const paged = (name, title, description, path, extraShape, toQuery, { localFilter = false, timeField = 'created_at' } = {}) => {
    const shape = { address: addr(), ...extraShape, limit: limitArg(), cursor: cursorArg, max_pages: maxPagesArg };
    if (localFilter) {
      shape.start_time = timeArg('Only rows at/after this time');
      shape.end_time = timeArg('Only rows at/before this time');
    }
    def({
      name,
      title,
      description: description + PAGING_NOTE + (localFilter ? LOCAL_FILTER_NOTE : ''),
      shape,
      run: async (a) => {
        const startMs = localFilter ? parseTimeToMs(a.start_time) : undefined;
        const endMs = localFilter ? parseTimeToMs(a.end_time) : undefined;
        const hasFilter = startMs !== undefined || endMs !== undefined;
        const maxPages = a.max_pages !== undefined ? a.max_pages : hasFilter ? 3 : 1;
        const query = { account: a.address, ...(toQuery ? toQuery(a) : {}), limit: a.limit, cursor: a.cursor };
        return apiGetPaged(path, query, { maxPages, startMs, endMs, timeField });
      }
    });
  };

  /* ---------------- Account ---------------- */

  simple(
    'pacifica_get_account',
    'Get Pacifica account info',
    'Get balance, equity, fee level, margin used, cross MMR, available-to-spend/withdraw, pending balance, order counts and spot balances for ANY Pacifica account by wallet address. Use this first when investigating a ticket about a specific wallet.',
    '/account',
    { address: addr() },
    (a) => ({ account: a.address })
  );

  simple(
    'pacifica_get_positions',
    'Get Pacifica open positions',
    'Get an account\'s current open positions (symbol, side bid=long/ask=short, amount, entry price, margin for isolated positions, funding, liquidation_price when applicable). Current state only - use pacifica_get_trade_history for closed trades.',
    '/positions',
    { address: addr() },
    (a) => ({ account: a.address })
  );

  def({
    name: 'pacifica_get_position_liquidation_prices',
    title: 'Get Pacifica position liquidation prices',
    description:
      'Get the exchange-computed liquidation price for an account\'s currently OPEN positions (symbol, side, amount, entry_price, liquidation_price). Current state only, not historical. Use it to cross-check your own liquidation math (see pacifica_get_market_info for max_leverage -> MMR = 1/(2*max_leverage)).',
    shape: {
      address: addr(),
      symbol: z.string().optional().describe('Filter to one market symbol'),
      is_isolated: z.boolean().optional().describe('Only isolated-margin positions (true) or cross (false)'),
      min_liq_price: z.number().optional().describe('Minimum liquidation price filter'),
      max_liq_price: z.number().optional().describe('Maximum liquidation price filter'),
      limit: limitArg()
    },
    // NOTE: this endpoint takes `address`, not `account`.
    run: async (a) =>
      (
        await apiGet('/position_liquidation_prices', {
          address: a.address,
          symbol: a.symbol,
          is_isolated: a.is_isolated,
          min_liq_price: a.min_liq_price,
          max_liq_price: a.max_liq_price,
          limit: a.limit
        })
      ).json
  });

  simple(
    'pacifica_get_account_settings',
    'Get Pacifica account settings',
    'Get an account\'s non-default margin/leverage settings, spot settings, auto_lend_disabled, and use_ltp_for_stop_orders (explains "why did my stop trigger off a different price than I expected": when true, stops use last-traded price instead of the mark-price default).',
    '/account/settings',
    { address: addr() },
    (a) => ({ account: a.address })
  );

  simple(
    'pacifica_get_account_loan',
    'Get Pacifica account loan/collateral detail',
    'Get an account\'s borrow/collateral detail under unified margin: borrowed amount, pending interest, collateral utilization, per-asset LTV. Relevant to margin/liquidation questions involving spot collateral.',
    '/account/loan',
    { address: addr() },
    (a) => ({ account: a.address })
  );

  def({
    name: 'pacifica_get_equity_history',
    title: 'Get Pacifica account equity/PnL history',
    description:
      'Get an account\'s equity and PnL series over time - the same data the dashboard charts use. time_range is REQUIRED. account_equity = balance + unrealized PnL at that moment. pnl is cumulative since account creation (so PnL over a period = difference between two points). To see "what was this account worth at time X", query a narrow start_time/end_time window around X.' +
      PAGING_NOTE,
    shape: {
      address: addr(),
      time_range: z.enum(['1d', '7d', '14d', '30d', 'all']).describe('REQUIRED. Time window of history: 1d, 7d, 14d, 30d or all'),
      start_time: timeArg('Optional start of window'),
      end_time: timeArg('Optional end of window'),
      limit: limitArg(0, ' (default 100 data points)'),
      cursor: cursorArg,
      max_pages: maxPagesArg
    },
    run: async (a) =>
      apiGetPaged(
        '/portfolio',
        {
          account: a.address,
          time_range: a.time_range,
          start_time: parseTimeToMs(a.start_time),
          end_time: parseTimeToMs(a.end_time),
          limit: a.limit,
          cursor: a.cursor
        },
        { maxPages: a.max_pages || 1 }
      )
  });

  paged(
    'pacifica_get_balance_history',
    'Get Pacifica account balance ledger',
    'Get an account\'s balance ledger: every event with the amount and the running balance after it. By default only deposit, withdraw, subaccount_transfer and payout events. Set include_trades=true for the FULL ledger - also trade (fees + realized PnL), funding, market_liquidation, backstop_liquidation, adl_liquidation and deposit_release - which lets you reconcile a balance gap event by event. Use this for "where did my money go" and "where is my deposit/withdrawal" tickets.',
    '/account/balance/history',
    { include_trades: z.boolean().optional().describe('true = include trade, funding and liquidation events (full ledger). Default false (only deposits, withdrawals, transfers, payouts)') },
    (a) => ({ include_trades: a.include_trades }),
    { localFilter: true }
  );

  def({
    name: 'pacifica_get_trade_history',
    title: 'Get Pacifica account trade history',
    description:
      'Get an account\'s fills (one row per fill): symbol, side (open_long/open_short/close_long/close_short), amount, price, entry_price, fee, realized pnl, event_type (fulfill_taker/fulfill_maker), order_id, and cause (normal / market_liquidation / backstop_liquidation / settlement=ADL). Filter by symbol, builder_code, start_time/end_time (the API allows at most a 30-day range when both are given). Use limit up to 4000.' +
      PAGING_NOTE,
    shape: {
      address: addr(),
      symbol: z.string().optional().describe('Only fills for this market symbol'),
      builder_code: z.string().optional().describe('Only fills placed with this builder code'),
      start_time: timeArg('Start of window (max 30-day range with end_time)'),
      end_time: timeArg('End of window (max 30-day range with start_time)'),
      limit: limitArg(4000),
      cursor: cursorArg,
      max_pages: maxPagesArg
    },
    run: async (a) =>
      apiGetPaged(
        '/trades/history',
        {
          account: a.address,
          symbol: a.symbol,
          builder_code: a.builder_code,
          start_time: parseTimeToMs(a.start_time),
          end_time: parseTimeToMs(a.end_time),
          limit: a.limit,
          cursor: a.cursor
        },
        { maxPages: a.max_pages || 1 }
      )
  });

  paged(
    'pacifica_get_account_funding_history',
    'Get Pacifica account funding payments',
    'Get what a SPECIFIC account actually paid or received in funding (history_id, symbol, side, position amount, payout in USD, rate, created_at), newest first. Not the market rate - use pacifica_get_funding_rate_history for that. Settles "I was charged funding I did not expect".',
    '/funding/history',
    {},
    null,
    { localFilter: true }
  );

  paged(
    'pacifica_get_deposit_history',
    'Get Pacifica USDC deposit history',
    'Get an account\'s USDC/perp-account deposit history (amount, transaction_id, created_at). For spot-asset deposits use pacifica_get_spot_deposit_history.',
    '/account/deposit/history',
    {},
    null,
    { localFilter: true }
  );

  paged(
    'pacifica_get_withdrawal_history',
    'Get Pacifica USDC withdrawal history',
    'Get an account\'s completed USDC/perp-account withdrawals (amount, batch_nonce, transaction_id, created_at). For still-processing ones use pacifica_get_pending_withdrawals. For spot assets use pacifica_get_spot_withdraw_history.',
    '/account/withdraw/history',
    {},
    null,
    { localFilter: true }
  );

  simple(
    'pacifica_get_pending_withdrawals',
    'Get Pacifica pending USDC withdrawals',
    'Get an account\'s PENDING USDC/perp-account withdrawals that have not yet been batched on-chain (amount, batch_nonce, created_at). The direct answer to "my withdrawal has been stuck for X": is it still queued, and since when. For spot assets use pacifica_get_spot_withdraw_pending.',
    '/account/withdraw/pending',
    { address: addr() },
    (a) => ({ account: a.address })
  );

  paged(
    'pacifica_get_payout_history',
    'Get Pacifica payout history',
    'Get an account\'s payout history (e.g. affiliate/referral payouts: amount, created_at).',
    '/account/payout/history',
    {},
    null,
    { localFilter: true }
  );

  /* ---------------- Orders ---------------- */

  simple(
    'pacifica_get_open_orders',
    'Get Pacifica open orders',
    'Get all currently open/pending orders for an account, including resting stop/TP/SL orders that have not triggered (order_id, symbol, side, price, initial/filled/cancelled amount, order_type, stop_price, reduce_only). For closed/filled/cancelled orders use pacifica_get_order_history or pacifica_get_order_history_by_id.',
    '/orders',
    { address: addr() },
    (a) => ({ account: a.address })
  );

  paged(
    'pacifica_get_order_history',
    'Get Pacifica account order history (list)',
    'Get an account\'s summarized order history (oldest first; each row has order_id, symbol, side, price, amounts, order_type, order_status, stop_price, reduce_only, reason). Browse without needing an order_id; then chain into pacifica_get_order_history_by_id for one order\'s full event history. Limit can be up to 4000.',
    '/orders/history',
    {},
    null,
    { localFilter: true }
  );

  simple(
    'pacifica_get_order_history_by_id',
    'Get Pacifica order history by order ID',
    'Get the full event history of ONE order by order_id (order_type e.g. stop_loss_market/limit/market, stop_price, reduce_only, trigger_price_type = what the stop actually triggers on: mark_price, last_trade_price or mid_price, plus each make/fulfill/cancel event). Take order_ids from pacifica_get_trade_history. This is the only way to confirm how an SL/TP was configured.',
    '/orders/history_by_id',
    { order_id: z.union([z.number(), z.string()]).describe('The order_id to look up') },
    (a) => ({ order_id: a.order_id })
  );

  def({
    name: 'pacifica_get_twap_orders',
    title: 'Get Pacifica TWAP orders',
    description:
      'TWAP order lookups for an account. view="open" = currently running TWAP orders; view="history" = summarized TWAP order history (paged); view="by_id" = full per-event history of one TWAP order (needs order_id).' +
      PAGING_NOTE,
    shape: {
      view: z.enum(['open', 'history', 'by_id']).describe('open | history | by_id'),
      address: addr(false),
      order_id: z.number().int().min(0).optional().describe('Required for view="by_id"'),
      limit: limitArg(4000),
      cursor: cursorArg,
      max_pages: maxPagesArg
    },
    run: async (a) => {
      if (a.view === 'by_id') {
        if (a.order_id === undefined) throw new Error('order_id is required for view="by_id".');
        return (await apiGet('/orders/twap/history_by_id', { order_id: a.order_id })).json;
      }
      if (!a.address) throw new Error('address is required for this view.');
      if (a.view === 'open') return (await apiGet('/orders/twap', { account: a.address })).json;
      return apiGetPaged('/orders/twap/history', { account: a.address, limit: a.limit, cursor: a.cursor }, { maxPages: a.max_pages || 1 });
    }
  });

  /* ---------------- Spot ---------------- */

  paged(
    'pacifica_get_spot_balance_history',
    'Get Pacifica spot balance history',
    'Get an account\'s spot-asset balance history (symbol, amount, balance after, event_type, created_at). Optionally for one asset.',
    '/account/spot_balance/history',
    { symbol: z.string().optional().describe('Spot asset symbol, e.g. SOL (omit for all assets)') },
    (a) => ({ symbol: a.symbol }),
    { localFilter: true }
  );

  paged(
    'pacifica_get_spot_deposit_history',
    'Get Pacifica spot deposit history',
    'Get an account\'s spot-asset deposit history (symbol, amount, transaction_id, created_at).',
    '/account/spot_asset/deposit/history',
    {},
    null,
    { localFilter: true }
  );

  paged(
    'pacifica_get_spot_withdraw_history',
    'Get Pacifica spot withdrawal history',
    'Get an account\'s completed spot-asset withdrawals (symbol, amount, batch_nonce, transaction_id, created_at). For still-pending ones use pacifica_get_spot_withdraw_pending.',
    '/account/spot_asset/withdraw/history',
    {},
    null,
    { localFilter: true }
  );

  simple(
    'pacifica_get_spot_withdraw_pending',
    'Get Pacifica pending spot withdrawals',
    'Get an account\'s PENDING spot-asset withdrawals not yet batched (symbol, amount, amount_requested, fee_amount, batch_nonce, created_at). For USDC use pacifica_get_pending_withdrawals.',
    '/account/spot_asset/withdraw/pending',
    { address: addr() },
    (a) => ({ account: a.address })
  );

  simple(
    'pacifica_get_spot_assets',
    'Get Pacifica spot assets',
    'List spot assets on Pacifica with trading parameters and collateral eligibility (symbol, tick/lot size, active, collateral_enabled, ltv_ratio). Answers "is X supported / usable as collateral".',
    '/spot_assets',
    {
      include_inactive: z.boolean().optional().describe('Include inactive assets (default false)'),
      collateral_enabled_only: z.boolean().optional().describe('Only assets enabled as collateral (default false)')
    },
    (a) => ({ include_inactive: a.include_inactive, collateral_enabled_only: a.collateral_enabled_only })
  );

  def({
    name: 'pacifica_get_bridge_info',
    title: 'Get Pacifica spot bridge parameters',
    description:
      'Bridge parameters for spot assets: minimum_deposit, withdrawal_fee, bridge_program, mint, decimals. Pass symbol for one asset, or omit for all. Answers "what is the minimum deposit / withdrawal fee for X".',
    shape: { symbol: z.string().optional().describe('Spot asset symbol, e.g. SOL. Omit for all assets') },
    run: async (a) =>
      (await apiGet(a.symbol ? `/spot_assets/bridge/parameters/${encodeURIComponent(a.symbol)}` : '/spot_assets/bridge/info', {})).json
  });

  /* ---------------- Markets ---------------- */

  simple(
    'pacifica_get_market_info',
    'Get Pacifica market info (leverage, tick/lot size, MMR)',
    'Per-market trading parameters for every market: max_leverage, tick_size, lot_size, min/max order size (USD), isolated_only, funding rate. On Pacifica maintenance margin is ALWAYS half the initial margin and initial margin = 1/max_leverage, so MMR for a market = 1/(2*max_leverage) (e.g. max_leverage 50 -> 1%). Pacifica\'s published formula: liquidation_price = [price - (side * position_margin) / position_size] / (1 - side / max_leverage / 2) (side = 1 long, -1 short; position_margin = account equity for cross margin).',
    '/info',
    {},
    null
  );

  simple(
    'pacifica_get_prices',
    'Get Pacifica live prices (all markets)',
    'Live snapshot of every market at once: mark, mid, oracle, funding, next funding, open interest, 24h volume, yesterday\'s price. One call instead of querying symbols one by one.',
    '/info/prices',
    {},
    null
  );

  simple(
    'pacifica_get_orderbook',
    'Get Pacifica order book',
    'Current order book snapshot for a symbol (bids and asks: price, amount, order count). Use to check liquidity/slippage claims - e.g. whether a thin book explains a worse-than-expected fill.',
    '/book',
    { symbol: z.string().describe('Market symbol, e.g. BTC (case-sensitive)'), agg_level: z.number().int().optional().describe('Price aggregation level (default 1)') },
    (a) => ({ symbol: a.symbol, agg_level: a.agg_level })
  );

  simple(
    'pacifica_get_recent_trades',
    'Get Pacifica market-wide recent trades',
    'Most recent public trades for a symbol - the venue-wide tape, NOT one account\'s fills (use pacifica_get_trade_history for that). Helps check whether a claimed fill price was possible at that time.',
    '/trades',
    { symbol: z.string().describe('Market symbol, e.g. BTC (case-sensitive)') },
    (a) => ({ symbol: a.symbol })
  );

  simple(
    'pacifica_get_klines',
    'Get Pacifica trade-price candles',
    'Historical candles built from LAST TRADED price (what the chart shows). Compare with pacifica_get_mark_price_klines: charts use trade price, but liquidations and stops trigger on mark price. Candle fields: t/T start/end ms, o/h/l/c, v volume, n trades.',
    '/kline',
    {
      symbol: z.string().describe('Market symbol, e.g. BTC (case-sensitive)'),
      interval: z.enum(KLINE_INTERVALS).describe('Candle interval; use 1m for trigger-time investigations'),
      start_time: z.union([z.number(), z.string()]).describe('Start of window. ms epoch or ISO-8601 string such as 2026-08-12T12:30:00Z'),
      end_time: timeArg('End of window (defaults to now)')
    },
    (a) => ({ symbol: a.symbol, interval: a.interval, start_time: parseTimeToMs(a.start_time), end_time: parseTimeToMs(a.end_time) })
  );

  simple(
    'pacifica_get_mark_price_klines',
    'Get Pacifica mark-price candles',
    'Historical MARK PRICE candles (1-minute granularity available). Mark price - not last-traded price - is what triggers liquidations and stop-loss/take-profit orders, so this is the authoritative source for "did mark price really reach this level at this time". Volume is always 0 for mark candles.',
    '/kline/mark',
    {
      symbol: z.string().describe('Market symbol, e.g. BTC (case-sensitive)'),
      interval: z.enum(KLINE_INTERVALS).describe('Candle interval; use 1m for trigger-time investigations'),
      start_time: z.union([z.number(), z.string()]).describe('Start of window. ms epoch or ISO-8601 string such as 2026-08-12T12:30:00Z'),
      end_time: timeArg('End of window (defaults to now)'),
      limit: limitArg(0, ' of candles')
    },
    (a) => ({
      symbol: a.symbol,
      interval: a.interval,
      start_time: parseTimeToMs(a.start_time),
      end_time: parseTimeToMs(a.end_time),
      limit: a.limit
    })
  );

  simple(
    'pacifica_get_funding_rate_history',
    'Get Pacifica market funding rate history',
    'Historical funding RATE for a market (oracle price, bid/ask impact price, funding_rate, next_funding_rate per hourly epoch). Market-level, not what an account paid - use pacifica_get_account_funding_history for that.' +
      PAGING_NOTE,
    '/funding_rate/history',
    { symbol: z.string().describe('Market symbol (case-sensitive)'), limit: limitArg(4000), cursor: cursorArg },
    (a) => ({ symbol: a.symbol, limit: a.limit, cursor: a.cursor })
  );

  simple(
    'pacifica_get_aggregated_funding_rates',
    'Get aggregated funding rates across exchanges',
    'Funding rates for each symbol across other exchanges (Pacifica\'s aggregate), for "why is funding so different from X" comparisons.',
    '/funding_rate/aggregated',
    {},
    null
  );

  simple(
    'pacifica_get_fees',
    'Get Pacifica fee schedule',
    'Fee tiers with maker and taker fee rates (level, maker_fee_rate, taker_fee_rate). Check fee disputes against these published rates; an account\'s tier is fee_level from pacifica_get_account.',
    '/info/fees',
    {},
    null
  );

  simple(
    'pacifica_get_loan_pool',
    'Get Pacifica loan pool info',
    'Market-level lending pool state for unified-margin borrow/lend: total borrowed, borrowable, utilization, borrow/lend APR and APY. For one account\'s borrow position use pacifica_get_account_loan.',
    '/loan_pool',
    {},
    null
  );

  simple(
    'pacifica_get_vaults',
    'Get Pacifica Vaults',
    'List all Pacifica Vaults (address, creator, manager, nickname, shares, balances, equity, high watermark, config such as deposit cap and profit share). Pass an account address to also get that wallet\'s share balance in each vault (user_share).',
    '/lake/list',
    { address: addr(false) },
    (a) => ({ account: a.address })
  );

  /* ---------------- Docs, status, utilities ---------------- */

  def({
    name: 'pacifica_search_docs',
    title: 'Search Pacifica official docs',
    description:
      'Search Pacifica\'s official documentation (docs.pacifica.fi) - trading rules, liquidations, order types, fees, Print, Vaults, API behavior, support articles such as "Why was I liquidated?" and "My TP/SL did not execute properly". Returns the best matching pages with a snippet and the official URL. Cite these URLs (they are verified, not guessed). Use pacifica_read_doc to read a whole page.',
    shape: {
      query: z.string().min(2).describe('What to look for, e.g. "why was I liquidated" or "TWAP order slippage"'),
      limit: z.number().int().min(1).max(10).optional().describe('Number of pages to return (default 5)')
    },
    run: async (a) => {
      const index = await getDocsIndex();
      const stems = tokenize(a.query);
      if (!stems.length) throw new Error('Please include at least one meaningful search word.');
      const phrase = String(a.query).toLowerCase().trim();
      const ranked = index.pages
        .map((p) => ({ p, score: scorePage(p, stems, phrase) }))
        .filter((x) => x.score > 0)
        .sort((x, y) => y.score - x.score)
        .slice(0, a.limit || 5);
      return {
        query: a.query,
        results: ranked.map((x) => ({
          title: x.p.title,
          url: x.p.url,
          score: x.score,
          summary: x.p.desc || undefined,
          snippet: bestSnippet(x.p.plain, stems)
        })),
        index: { pages_indexed: index.pages.length, built_at: new Date(index.builtAt).toISOString() },
        tip: ranked.length ? 'Open a result in full with pacifica_read_doc using its url.' : 'No matching pages. Try different or fewer keywords.'
      };
    }
  });

  def({
    name: 'pacifica_read_doc',
    title: 'Read a Pacifica docs page',
    description:
      'Read the full text of one official Pacifica docs page by URL or path (as returned by pacifica_search_docs), e.g. "https://docs.pacifica.fi/trading-on-pacifica/liquidations". Long pages are returned in chunks: use offset/next_offset to continue.',
    shape: {
      path_or_url: z.string().describe('A docs.pacifica.fi URL or path, e.g. /trading-on-pacifica/liquidations'),
      offset: z.number().int().min(0).optional().describe('Character offset to start from (default 0)'),
      max_chars: z.number().int().min(500).max(30000).optional().describe('Max characters to return (default 12000)')
    },
    run: async (a) => {
      const index = await getDocsIndex();
      let p = String(a.path_or_url).trim();
      if (/^https?:/i.test(p)) {
        let u;
        try {
          u = new URL(p);
        } catch (e) {
          throw new Error('That does not look like a valid URL.');
        }
        if (u.hostname !== 'docs.pacifica.fi') throw new Error('Only docs.pacifica.fi pages can be read with this tool.');
        p = u.pathname;
      }
      p = p.split('#')[0].split('?')[0].replace(/\.md$/, '').replace(/\/$/, '');
      if (!p.startsWith('/')) p = '/' + p;
      const page = index.byPath.get(p);
      if (!page) throw new Error('That page is not in the docs index. Use pacifica_search_docs to find the right page.');
      const offset = a.offset || 0;
      const size = a.max_chars || 12000;
      const slice = page.md.slice(offset, offset + size);
      const next = offset + slice.length;
      return {
        title: page.title,
        url: page.url,
        total_chars: page.md.length,
        offset,
        returned_chars: slice.length,
        truncated: next < page.md.length,
        next_offset: next < page.md.length ? next : null,
        content: slice
      };
    }
  });

  def({
    name: 'pacifica_get_status',
    title: 'Get Pacifica status page',
    description:
      'Check status.pacifica.fi: section="current" = live service status and recent uptime, "incidents" = incident history, "maintenance" = scheduled maintenance. Use it to check for outages or maintenance around a customer\'s timestamps. Cite https://status.pacifica.fi as the source.',
    shape: { section: z.enum(['current', 'incidents', 'maintenance']).optional().describe('Default "current"') },
    run: async (a) => fetchStatus(a.section || 'current')
  });

  def({
    name: 'pacifica_convert_time',
    title: 'Convert timestamps (epoch <-> UTC)',
    description:
      'Convert timestamps exactly, instead of doing epoch arithmetic by hand. Accepts millisecond epochs (13 digits), second epochs (10 digits) or ISO-8601 strings, and returns epoch_ms, epoch_seconds, iso_utc and weekday. Use it whenever you read or quote a created_at/timestamp value.',
    shape: { values: z.array(z.union([z.number(), z.string()])).min(1).max(50).describe('One or more timestamps to convert') },
    run: async (a) => ({
      conversions: a.values.map((v) => {
        let ms;
        if (typeof v === 'number' || /^\d+$/.test(String(v).trim())) {
          const n = Number(v);
          ms = n < 1e11 ? n * 1000 : n;
        } else {
          ms = Date.parse(String(v).trim());
          if (Number.isNaN(ms)) return { input: v, error: 'Could not parse this value.' };
        }
        const d = new Date(ms);
        return {
          input: v,
          epoch_ms: ms,
          epoch_seconds: Math.floor(ms / 1000),
          iso_utc: d.toISOString(),
          weekday_utc: ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'][d.getUTCDay()]
        };
      })
    })
  });
}

function buildServer() {
  const server = new McpServer({ name: 'pacifica-account-lookup', version: '2.0.0' });
  registerTools(server);
  return server;
}

/* ------------------------------------------------------------------ */
/* HTTP server (stateless MCP endpoint)                                */
/* ------------------------------------------------------------------ */

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
  res.send('Pacifica Lookup MCP server is running. Point your MCP client at /mcp');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Pacifica lookup MCP server listening on port ${PORT}`);
  // Warm the docs index in the background so the first docs search is fast.
  getDocsIndex().catch((e) => console.error('Docs warm-up failed (will retry on demand):', e.message));
});
