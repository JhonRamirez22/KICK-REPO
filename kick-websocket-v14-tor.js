#!/usr/bin/env node
/**
 * Kick Viewer Bot v14.0 — API Server Edition
 * ═══════════════════════════════════════════════════════════════
 * Obtiene tokens via Token Server API (que usa Tor SOCKS5 proxy).
 *
 * USO: node kick-websocket-v14-tor.js "https://kick.com/canal" [viewers]
 * REQ: npm install ws  &&  python3 token_server_vps.py
 */

'use strict';

const WebSocket     = require('ws');
const http          = require('http');
const path          = require('path');
const fs            = require('fs');

const TOKEN_API     = 'http://127.0.0.1:8765';
const WS_CONNECT    = 'wss://websockets.kick.com/viewer/v1/connect';
const PUSHER_KEY    = '32cbd69e4b950bf97679';
const PUSHER_WS     = `wss://ws-us2.pusher.com/app/${PUSHER_KEY}?protocol=7&client=js&version=8.4.0&flash=false`;

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
];

function rand(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }
function rndUA()        { return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]; }
function pad(s, n)      { return String(s).padEnd(n); }

function isValidToken(t) {
    return typeof t === 'string' && t.trim().length > 20;
}

// ── API helper ─────────────────────────────────────────────────────────
function apiGet(urlPath) {
    return new Promise((resolve, reject) => {
        const req = http.get(`${TOKEN_API}${urlPath}`, (res) => {
            let data = '';
            res.on('data', c => data += c);
            res.on('end', () => {
                try { resolve(JSON.parse(data)); }
                catch (e) { resolve({ error: 'parse error' }); }
            });
        });
        req.on('error', reject);
        req.setTimeout(30000);
        req.end();
    });
}

// ════════════════════════════════════════════════════════════════════════

class KickViewerBot {
    constructor(streamUrl, viewerCount) {
        this.streamUrl   = streamUrl;
        this.viewerCount = viewerCount;
        this.streamName  = streamUrl.replace(/\/$/, '').split('/').pop();
        this.channelId   = null;
        this.chatroomId  = null;
        this.startTime   = Date.now();
        this.sockets     = [];
        this.tokenPool   = [];
        this.state = {
            tokens:    0,
            connected: 0,
            handshake: 0,
            pusher:    0,
            failed:    0,
            reconnects: 0,
            poolSize:  0,
            refreshes: 0,
        };
    }

    formatUptime() {
        const s = Math.floor((Date.now() - this.startTime) / 1000);
        return `${String(Math.floor(s/3600)).padStart(2,'0')}:${String(Math.floor((s%3600)/60)).padStart(2,'0')}:${String(s%60).padStart(2,'0')}`;
    }
    memUsage() { return `${(process.memoryUsage().rss/1024/1024).toFixed(1)} MB`; }

    async getChannelInfo() {
        console.log('\n  [1/3] Obteniendo info del canal via API...');
        try {
            const data = await apiGet(`/discover?channel=${this.streamName}`);
            if (data.error) { console.log(`  API error: ${data.error}`); return false; }
            this.channelId  = String(data.id);
            this.chatroomId = data.chatroom_id ? String(data.chatroom_id) : null;
            console.log(`  Channel ID  : ${this.channelId}`);
            console.log(`  Chatroom ID : ${this.chatroomId || '???'}`);
            console.log(`  En vivo     : ${data.live ? `SI 🔴 (${data.viewers} viewers) — ${data.title}` : 'NO ⚫'}`);
            return true;
        } catch (e) {
            console.log(`  Error API: ${e.message.split('\n')[0]}`);
            return false;
        }
    }

    async getViewerTokens(count) {
        console.log(`\n  Obteniendo ${count} tokens via API Server (Tor proxy)...`);
        let allTokens = [];
        const batchSize = 100;
        const batches = Math.ceil(count / batchSize);
        for (let i = 0; i < batches; i++) {
            const size = Math.min(batchSize, count - allTokens.length);
            try {
                const data = await apiGet(`/batch-tokens?channel=${this.streamName}&count=${size}`);
                const tokens = (data.tokens || []).filter(isValidToken);
                allTokens.push(...tokens);
                console.log(`  >> Progreso: ${allTokens.length}/${count}`);
            } catch (e) {
                console.warn(`  ⚠️  Batch ${i + 1} falló: ${e.message.slice(0, 50)}`);
            }
            if (allTokens.length >= count) break;
            await new Promise(r => setTimeout(r, 2000));
        }
        this.state.tokens = allTokens.length;
        console.log(`  ✅ Total: ${allTokens.length}/${count} tokens`);
        return allTokens;
    }

    async getSingleToken() {
        try {
            const data = await apiGet(`/token?channel=${this.streamName}`);
            return data.token || null;
        } catch (_) {
            return null;
        }
    }

    connectViewer(index, token) {
        const ua = rndUA();

        function connectViewerWS(tkn) {
            if (!isValidToken(tkn)) {
                this.getSingleToken().then(fresh => {
                    if (fresh) connectViewerWS.call(this, fresh);
                    else setTimeout(() => {
                        this.getSingleToken(f2 => { if (f2) connectViewerWS.call(this, f2); });
                    }, rand(10000, 30000));
                });
                return;
            }

            const ws = new WebSocket(`${WS_CONNECT}?token=${tkn}`, {
                headers: { 'User-Agent': ua, 'Origin': 'https://kick.com' },
                rejectUnauthorized: false,
            });

            let pingInterval      = null;
            let handshakeInterval = null;

            const handshake = JSON.stringify({
                type: 'channel_handshake',
                data: { message: { channelId: this.channelId } },
            });

            ws.on('open', () => {
                this.state.connected++;
                this.sockets.push(ws);
                ws.send(handshake);
                this.state.handshake++;

                handshakeInterval = setInterval(() => {
                    try { if (ws.readyState === WebSocket.OPEN) ws.send(handshake); } catch (_) {}
                }, 15000);

                pingInterval = setInterval(() => {
                    try { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'ping' })); } catch (_) {}
                }, 30000);
            });

            ws.on('message', () => {});
            ws.on('error', () => {});

            ws.on('close', (code) => {
                this.state.connected = Math.max(0, this.state.connected - 1);
                this.sockets = this.sockets.filter(s => s !== ws);
                if (pingInterval)      { clearInterval(pingInterval);      pingInterval = null; }
                if (handshakeInterval) { clearInterval(handshakeInterval); handshakeInterval = null; }
                if (code === 1000) return;

                this.state.reconnects++;
                setTimeout(() => {
                    this.getSingleToken().then(fresh => {
                        if (fresh) connectViewerWS.call(this, fresh);
                        else setTimeout(() => {
                            this.getSingleToken(f2 => { if (f2) connectViewerWS.call(this, f2); });
                        }, rand(10000, 30000));
                    });
                }, Math.min(60000, rand(2000, 5000)));
            });
        }

        function connectPusherForViewer() {
            const pws = new WebSocket(PUSHER_WS, {
                headers: { 'User-Agent': ua, 'Origin': 'https://kick.com' },
                rejectUnauthorized: false,
            });

            let pusherPing = null;

            pws.on('message', (data) => {
                try {
                    const msg = JSON.parse(data.toString());
                    if (msg.event === 'pusher:connection_established') {
                        [
                            `channel_${this.channelId}`,
                            `channel.${this.channelId}`,
                            `chatrooms.${this.chatroomId || this.channelId}.v2`,
                            `chatroom_${this.chatroomId || this.channelId}`,
                            `chatrooms.${this.chatroomId || this.channelId}`,
                            'drops_category_8',
                        ].forEach(ch => {
                            pws.send(JSON.stringify({ event: 'pusher:subscribe', data: { auth: '', channel: ch } }));
                        });
                        this.state.pusher++;
                        this.sockets.push(pws);
                    }
                } catch (_) {}
            });

            pws.on('open', () => {
                pusherPing = setInterval(() => {
                    try { if (pws.readyState === WebSocket.OPEN) pws.send(JSON.stringify({ event: 'pusher:ping', data: {} })); } catch (_) {}
                }, 110000);
            });

            pws.on('close', () => {
                this.state.pusher = Math.max(0, this.state.pusher - 1);
                this.sockets = this.sockets.filter(s => s !== pws);
                if (pusherPing) { clearInterval(pusherPing); pusherPing = null; }
                setTimeout(() => connectPusherForViewer.call(this), rand(3000, 10000));
            });

            pws.on('error', () => {});
        }

        connectViewerWS.call(this, token);
        connectPusherForViewer.call(this);

        if (index <= 3) {
            console.log(`[✅] Viewer ${index}: ViewerWS + PusherWS lanzados`);
        }
    }

    async start() {
        console.clear();
        console.log('\n╔══════════════════════════════════════════════════════════╗');
        console.log('║   KICK VIEWER BOT v14.0 — Tor + API Server             ║');
        console.log('║   Tokens via Token Server → SOCKS5 Tor proxy            ║');
        console.log('╚══════════════════════════════════════════════════════════╝\n');
        console.log(`  Stream   : ${this.streamName}`);
        console.log(`  Viewers  : ${this.viewerCount}`);

        if (!await this.getChannelInfo()) {
            console.log('  ⚠️  No se pudo obtener channel ID');
            process.exit(1);
        }

        const tokens = await this.getViewerTokens(this.viewerCount);
        if (tokens.length === 0) {
            console.error('\n  ❌ No se obtuvieron tokens. Token server activo? systemctl status kick-token-server');
            process.exit(1);
        }

        // Pool extra
        const extraCount = Math.min(Math.ceil(this.viewerCount * 0.25), 500);
        const extraTokens = await this.getViewerTokens(extraCount);
        if (extraTokens.length > 0) {
            this.tokenPool = extraTokens;
            console.log(`  ✅ Token pool: ${this.tokenPool.length} tokens de reserva`);
        }

        console.log(`\n  [3/3] Conectando ${tokens.length} viewers...\n`);
        for (let i = 0; i < tokens.length; i++) {
            this.connectViewer(i + 1, tokens[i]);
            const stagger = tokens.length > 1000
                ? 50  + Math.random() * 100
                : 250 + Math.random() * 350;
            await new Promise(r => setTimeout(r, stagger));
        }

        // Dashboard
        setInterval(() => this.updateDashboard(), 10000);

        console.log(`\n  ✅ ${tokens.length} viewers lanzados. Dashboard en 10s...`);
        console.log('  🔄 Token API: Tor proxy activo, IP rota cada 5 min');
    }

    updateDashboard() {
        this.state.poolSize = this.tokenPool.length;
        console.clear();
        console.log([
            '',
            '╔══════════════════════════════════════════════════════════╗',
            '║   KICK VIEWER BOT v14.0 — Tor Edition                 ║',
            '╠══════════════════════════════════════════════════════════╣',
            `║  Stream      : ${pad(this.streamName, 42)}║`,
            `║  Channel ID  : ${pad(this.channelId || '???', 42)}║`,
            '╠══════════════════════════════════════════════════════════╣',
            `║  Tokens      : ${pad(this.state.tokens, 42)}║`,
            `║  Token Pool  : ${pad(this.tokenPool.length + ' disponibles', 42)}║`,
            `║  ViewerWS    : ${pad(this.state.connected + ' / ' + this.viewerCount, 42)}║`,
            `║  PusherWS    : ${pad(this.state.pusher + ' / ' + this.viewerCount, 42)}║`,
            `║  Handshakes  : ${pad(this.state.handshake, 42)}║`,
            `║  Reconex.    : ${pad(this.state.reconnects, 42)}║`,
            `║  RAM         : ${pad(this.memUsage(), 42)}║`,
            `║  Uptime      : ${pad(this.formatUptime(), 42)}║`,
            '╠══════════════════════════════════════════════════════════╣',
            `║  Proxy mode  : ${pad('Tor SOCKS5 (IP rota c/5min)', 42)}║`,
            '╚══════════════════════════════════════════════════════════╝',
        ].join('\n'));
    }
}

// ── Main ──────────────────────────────────────────────────────────────
const targetUrl   = process.argv[2];
const targetCount = parseInt(process.argv[3]) || 50;

if (!targetUrl) {
    console.error('\nUSO: node kick-websocket-v14-tor.js <URL> [viewers]');
    console.error('EJ : node kick-websocket-v14-tor.js "https://kick.com/canal" 100\n');
    console.error('REQUISITOS:');
    console.error('  1. python3 token_server_vps.py  (API con Tor)');
    console.error('  2. npm install ws');
    process.exit(1);
}

const bot = new KickViewerBot(targetUrl, targetCount);

process.on('SIGINT', () => {
    console.log(`\nCerrando ${bot.sockets.length} WebSockets...`);
    bot.sockets.forEach(ws => { try { ws.close(1000); } catch (_) {} });
    setTimeout(() => process.exit(0), 2000);
});

process.on('uncaughtException', e => {
    console.error('[UNCAUGHT]', e.message);
});

bot.start().catch(e => console.error('Error fatal:', e));
setInterval(() => {}, 1000);
