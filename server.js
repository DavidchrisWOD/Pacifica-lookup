'use strict';

/**
 * Pacifica Account Lookup - remote (HTTP-hosted) MCP server.
 * Same tools as the local version, but reachable over the public
 * internet so Claude Mobile, claude.ai, and Claude Desktop can all
 * use it as a Custom Connector - not just one laptop.
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
