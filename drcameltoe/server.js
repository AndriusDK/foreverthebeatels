/**
 * DRCAMELTOE — local proxy server
 *
 * Run:  node server.js
 * Then: open http://localhost:3000 in your browser
 *
 * What it does:
 *  1. Serves index.html at http://localhost:3000
 *  2. Connects to wss://stream.pumpapi.io/ (server-side, no browser restrictions)
 *  3. Forwards all stream events to the browser via ws://localhost:3001
 */

const http  = require('http');
const https = require('https');
const fs    = require('fs');
const path  = require('path');
const { WebSocket, WebSocketServer } = require('ws');

const HTTP_PORT   = 3000;
const WS_PORT     = 3001;
const UPSTREAM_WS = 'wss://stream.pumpapi.io/';

// ── 1. HTTP server — serve index.html + proxy trade API ───────
const httpServer = http.createServer((req, res) => {
  // CORS headers so browser can call /trade freely
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // POST /trade — proxy to api.pumpapi.io
  if (req.method === 'POST' && req.url === '/trade') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      const options = {
        hostname: 'api.pumpapi.io',
        path: '/',
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
      };
      const proxy = https.request(options, (upstream) => {
        let data = '';
        upstream.on('data', chunk => data += chunk);
        upstream.on('end', () => {
          res.writeHead(upstream.statusCode, { 'Content-Type': 'application/json' });
          res.end(data);
          console.log(`[drcameltoe] Trade proxy → ${upstream.statusCode}`);
        });
      });
      proxy.on('error', (e) => {
        console.error('[drcameltoe] Trade proxy error:', e.message);
        res.writeHead(502); res.end(JSON.stringify({ error: e.message }));
      });
      proxy.write(body);
      proxy.end();
    });
    return;
  }

  // GET / — serve index.html
  const file = path.join(__dirname, 'index.html');
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); res.end('index.html not found'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(data);
  });
});

httpServer.listen(HTTP_PORT, () => {
  console.log(`[drcameltoe] UI ready at http://localhost:${HTTP_PORT}`);
});

// ── 2. Local WebSocket server — browser connects here ─────────
const wss = new WebSocketServer({ port: WS_PORT });
console.log(`[drcameltoe] Local WS proxy listening on ws://localhost:${WS_PORT}`);

// ── 3. Upstream connection to pumpapi.io ──────────────────────
let upstream     = null;
let reconnectTmo = null;
const clients    = new Set();

function connectUpstream() {
  console.log(`[drcameltoe] Connecting to ${UPSTREAM_WS}...`);

  upstream = new WebSocket(UPSTREAM_WS);

  upstream.on('open', () => {
    console.log('[drcameltoe] Stream connected');
    broadcast(JSON.stringify({ _proxy: 'connected' }));
  });

  upstream.on('message', (data) => {
    // Forward raw message to all connected browser clients
    broadcast(data.toString());
  });

  upstream.on('error', (err) => {
    console.error('[drcameltoe] Stream error:', err.message);
  });

  upstream.on('close', () => {
    console.log('[drcameltoe] Stream disconnected — reconnecting in 3s...');
    broadcast(JSON.stringify({ _proxy: 'disconnected' }));
    reconnectTmo = setTimeout(connectUpstream, 3000);
  });
}

function broadcast(msg) {
  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  });
}

// ── 4. Handle browser clients ─────────────────────────────────
wss.on('connection', (ws) => {
  clients.add(ws);
  console.log(`[drcameltoe] Browser connected (${clients.size} total)`);

  // Tell the browser the current upstream status
  if (upstream?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ _proxy: 'connected' }));
  }

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[drcameltoe] Browser disconnected (${clients.size} remaining)`);
  });
});

// Start upstream connection
connectUpstream();

process.on('SIGINT', () => {
  console.log('\n[drcameltoe] Shutting down...');
  clearTimeout(reconnectTmo);
  upstream?.close();
  process.exit(0);
});
