#!/usr/bin/env node
/**
 * Kick Viewer Bot v14.0
 * ═══════════════════════════════════════════════════════════════
 * Basado en v9.1 original — tokens intactos
 * Fix único aplicado:
 *   - stderr de Python visible en consola (nunca silencioso)
 *   - Guardia simple: nunca conectar con token vacío/string vacío
 *   - Reconexión siempre solicita token fresco del pool
 *
 * USO: node kick-websocket-v14.js "https://kick.com/canal" [viewers]
 * REQ: pip install curl_cffi  &&  npm install ws
 */

'use strict';

const WebSocket     = require('ws');
const { spawnSync } = require('child_process');
const path          = require('path');
const fs            = require('fs');

const CLIENT_TOKEN  = 'e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823';
const WS_CONNECT    = 'wss://websockets.kick.com/viewer/v1/connect';
const PUSHER_KEY    = '32cbd69e4b950bf97679';
const PUSHER_WS     = `wss://ws-us2.pusher.com/app/${PUSHER_KEY}?protocol=7&client=js&version=8.4.0&flash=false`;

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
];

function rand(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }
function rndUA()        { return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]; }
function pad(s, n)      { return String(s).padEnd(n); }

// Validación simple: solo verificar que el token no esté vacío.
// Los tokens de Kick NO son JWTs estándar, no parsear su estructura.
function isValidToken(t) {
    return typeof t === 'string' && t.trim().length > 20;
}

// Prefijo único por proceso — evita colisiones entre múltiples terminales
const PID_PREFIX = `_kv_${process.pid}_`;

// Escribe en carpeta temp del sistema, nunca en el dir del proyecto
function writePy(name, code) {
    const p = path.join(require('os').tmpdir(), PID_PREFIX + name);
    fs.writeFileSync(p, code, 'utf8');
    return p;
}

// Ejecuta Python: stdout capturado, stderr VISIBLE en consola
function runPy(scriptPath, timeoutMs) {
    const result = spawnSync('python3', [scriptPath], {
        timeout:   timeoutMs,
        encoding:  'utf8',
        maxBuffer: 1024 * 1024 * 100,
        stdio:     ['ignore', 'pipe', 'inherit'],   // stderr → pantalla siempre
    });
    if (result.error) throw result.error;
    return (result.stdout || '').trim();
}

// ══════════════════════════════════════════════════════════════════════════════
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
        this.isRefreshing = false;
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

    // ──────────────────────────────────────────────────────────────────────
    // getChannelInfo — idéntico a v9.1
    // ──────────────────────────────────────────────────────────────────────
    getChannelInfo() {
        const script = writePy('_channel_info.py', `
import json
from curl_cffi import requests as r
PROXY = {"http": "socks5://127.0.0.1:9050", "https": "socks5://127.0.0.1:9050"}
try:
    res = r.get("https://kick.com/api/v1/channels/${this.streamName}",
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0",
                 "Accept": "application/json"},
        timeout=30, impersonate="chrome131", proxies=PROXY)
    if res.status_code == 200:
        d = res.json()
        ls = d.get("livestream") or {}
        print(json.dumps({"id": d.get("id"),
            "chatroom_id": (d.get("chatroom") or {}).get("id"),
            "live": d.get("livestream") is not None,
            "viewers": ls.get("viewers", 0),
            "title": ls.get("session_title", "")}))
    else:
        print(json.dumps({"error": res.status_code}))
except Exception as e:
    print(json.dumps({"error": str(e)}))
`.trim());
        try {
            const data = JSON.parse(runPy(script, 25000));
            if (data.error) { console.log(`  API error: ${data.error}`); return false; }
            this.channelId  = String(data.id);
            this.chatroomId = data.chatroom_id ? String(data.chatroom_id) : null;
            console.log(`  Channel ID  : ${this.channelId}`);
            console.log(`  Chatroom ID : ${this.chatroomId || '???'}`);
            console.log(`  En vivo     : ${data.live ? `SI 🔴 (${data.viewers} viewers) — ${data.title}` : 'NO ⚫'}`);
            return true;
        } catch (e) {
            console.log(`  Error obteniendo info: ${e.message.split('\n')[0]}`);
            return false;
        } finally {
            try { fs.unlinkSync(script); } catch (_) {}
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // getViewerTokens — IDÉNTICO A V9.1, sin tocar nada
    // ──────────────────────────────────────────────────────────────────────
    getViewerTokens(count) {
        console.log(`\n  Obteniendo ${count} viewer tokens via curl_cffi...`);

        const BATCH_SIZE     = count > 200 ? 50 : 10;
        const MAX_CONCURRENT = count > 1000 ? 100 : 50;
        const STAGGER        = count > 1000 ? 0.2 : 0.5;

        const script = writePy('_get_tokens.py', `
import json, uuid, time, sys, threading
from curl_cffi import requests as cffi_requests

PROXY         = {"http": "socks5://127.0.0.1:9050", "https": "socks5://127.0.0.1:9050"}
CLIENT_TOKEN  = "${CLIENT_TOKEN}"
CHANNEL       = "${this.streamName}"
COUNT         = ${count}
BATCH         = ${BATCH_SIZE}
MAX_CONCURRENT = ${MAX_CONCURRENT}
STAGGER       = ${STAGGER}

UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
]
import random

all_tokens = []
lock = threading.Lock()
semaphore = threading.Semaphore(MAX_CONCURRENT)
progress = {"ok": 0, "fail": 0}

def get_batch(batch_id, num):
    with semaphore:
        ua = random.choice(UAS)
        try:
            session = cffi_requests.Session(impersonate="chrome131", proxies=PROXY)
                session.get(f"https://kick.com/{CHANNEL}",
                    headers={"User-Agent": ua, "Accept": "text/html,*/*",
                             "sec-fetch-dest": "document", "sec-fetch-mode": "navigate"},
                    timeout=30)
                break
            except Exception as e:
                retries += 1
                if retries < max_retries:
                    time.sleep(2 * retries)
                    session = None
                else:
                    print(f"[batch {batch_id}] SKIP: CF session fail after {max_retries} retries", file=sys.stderr)
                    return
        
        if session is None:
            return
        
        tokens = []
        for i in range(num):
            token_retry = 0
            while token_retry < 2:
                try:
                    r = session.get("https://websockets.kick.com/viewer/v1/token",
                        headers={
                            "User-Agent": ua,
                            "Accept": "application/json, text/plain, */*",
                            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131"',
                            "sec-ch-ua-mobile": "?0",
                            "sec-ch-ua-platform": '"Windows"',
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors",
                            "sec-fetch-site": "same-site",
                            "Origin": "https://kick.com",
                            "Referer": f"https://kick.com/{CHANNEL}",
                            "X-CLIENT-TOKEN": CLIENT_TOKEN,
                            "X-Device-ID": str(uuid.uuid4()),
                            "X-Session-ID": str(uuid.uuid4()),
                        }, timeout=45)
                    if r.status_code == 200:
                        token = r.json().get("data", {}).get("token", "")
                        if token:
                            tokens.append(token)
                        break
                    elif r.status_code == 429:
                        print(f"[batch {batch_id}] RATE LIMITED, sleep 5s", file=sys.stderr)
                        time.sleep(5)
                        break
                    else:
                        token_retry += 1
                        if token_retry < 2:
                            time.sleep(1)
                    break
                except Exception as e:
                    token_retry += 1
                    if token_retry < 2:
                        time.sleep(1)
                    else:
                        print(f"[batch {batch_id}] token ERR: {str(e)[:50]}", file=sys.stderr)
            time.sleep(0.2)
        
        with lock:
            all_tokens.extend(tokens)
            progress["ok"] += len(tokens)
            progress["fail"] += (num - len(tokens))
            total = progress["ok"] + progress["fail"]
            print(f"  >> Progreso: {progress['ok']}/{COUNT} tokens ({total} procesados)", file=sys.stderr)

num_batches = (COUNT + BATCH - 1) // BATCH
threads = []
for b in range(num_batches):
    start = b * BATCH
    num = min(BATCH, COUNT - start)
    t = threading.Thread(target=get_batch, args=(b, num))
    threads.append(t)
    t.start()
    time.sleep(STAGGER)

for t in threads:
    t.join(timeout=300)

print(json.dumps(all_tokens))
`.trim());

        try {
            const timeout = Math.max(120000, count * 100);
            const result  = runPy(script, timeout);

            if (!result) {
                console.error('  ❌ Script Python no produjo salida. Ver errores arriba ↑');
                return [];
            }

            const tokens = JSON.parse(result);
            // Solo filtrar strings vacíos — no tocar el contenido del token
            const valid = tokens.filter(isValidToken);

            this.state.tokens = valid.length;
            console.log(`  ✅ Tokens obtenidos: ${valid.length}/${count}`);
            if (tokens.length - valid.length > 0) {
                console.log(`  ⚠️  Descartados (vacíos): ${tokens.length - valid.length}`);
            }
            return valid;

        } catch (e) {
            console.error(`  ❌ Error: ${e.message.split('\n')[0]}`);
            try {
                const partial = JSON.parse((e.stdout || '').trim()).filter(isValidToken);
                if (partial.length > 0) {
                    this.state.tokens = partial.length;
                    console.log(`  ⚠️  Tokens parciales: ${partial.length}`);
                    return partial;
                }
            } catch (_) {}
            return [];
        } finally {
            try { fs.unlinkSync(script); } catch (_) {}
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // getSingleToken — idéntico a v9.1
    // ──────────────────────────────────────────────────────────────────────
    getSingleToken(callback) {
        const script = writePy('_single_token.py', `
import json, uuid
from curl_cffi import requests as r
PROXY = {"http": "socks5://127.0.0.1:9050", "https": "socks5://127.0.0.1:9050"}
try:
    s = r.Session(impersonate="chrome131", proxies=PROXY)
    s.get("https://kick.com/${this.streamName}",headers={"User-Agent":"Mozilla/5.0","Accept":"text/html,*/*"},timeout=30)
    res = s.get("https://websockets.kick.com/viewer/v1/token",
      headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0",
        "Accept":"application/json","Origin":"https://kick.com","Referer":"https://kick.com/${this.streamName}",
        "sec-fetch-site":"same-site","sec-fetch-mode":"cors","X-CLIENT-TOKEN":"${CLIENT_TOKEN}",
        "X-Device-ID":str(uuid.uuid4()),"X-Session-ID":str(uuid.uuid4())},timeout=45)
    print(res.json().get("data",{}).get("token","") if res.status_code==200 else "")
except:
    print("")
`.trim());
        try {
            const result = runPy(script, 30000);
            callback(isValidToken(result) ? result : null);
        } catch (_) {
            callback(null);
        } finally {
            try { fs.unlinkSync(script); } catch (_) {}
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // connectViewer — idéntico a v9.1 + guardia de token vacío
    // ──────────────────────────────────────────────────────────────────────
    connectViewer(index, token) {
        const ua = rndUA();

        function connectViewerWS(tkn) {
            // Guardia: nunca abrir WS con token vacío
            if (!isValidToken(tkn)) {
                const poolToken = this.tokenPool.shift();
                if (poolToken && isValidToken(poolToken)) {
                    connectViewerWS.call(this, poolToken);
                } else {
                    this.getSingleToken((fresh) => {
                        if (fresh) connectViewerWS.call(this, fresh);
                        else setTimeout(() => {
                            this.getSingleToken(f2 => { if (f2) connectViewerWS.call(this, f2); });
                        }, rand(10000, 30000));
                    });
                }
                return;
            }

            const ws = new WebSocket(`${WS_CONNECT}?token=${tkn}`, {
                headers: {
                    'User-Agent': ua,
                    'Origin':     'https://kick.com',
                },
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

                // Re-handshake cada 15s
                handshakeInterval = setInterval(() => {
                    try {
                        if (ws.readyState === WebSocket.OPEN) ws.send(handshake);
                    } catch (_) {}
                }, 15000);

                // Ping cada 30s
                pingInterval = setInterval(() => {
                    try {
                        if (ws.readyState === WebSocket.OPEN)
                            ws.send(JSON.stringify({ type: 'ping' }));
                    } catch (_) {}
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
                const delay = Math.min(60000, rand(2000, 5000));

                setTimeout(() => {
                    const poolToken = this.tokenPool.shift();
                    if (poolToken && isValidToken(poolToken)) {
                        connectViewerWS.call(this, poolToken);
                    } else {
                        this.getSingleToken((fresh) => {
                            if (fresh) connectViewerWS.call(this, fresh);
                            else setTimeout(() => connectViewerWS.call(this, tkn), rand(10000, 30000));
                        });
                    }
                }, delay);
            });
        }

        // ── Pusher — idéntico a v9.1 ─────────────────────────────────
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
                        const channels = [
                            `channel_${this.channelId}`,
                            `channel.${this.channelId}`,
                            `chatrooms.${this.chatroomId || this.channelId}.v2`,
                            `chatroom_${this.chatroomId || this.channelId}`,
                            `chatrooms.${this.chatroomId || this.channelId}`,
                            'drops_category_8',
                        ];
                        channels.forEach(ch => {
                            pws.send(JSON.stringify({ event: 'pusher:subscribe', data: { auth: '', channel: ch } }));
                        });
                        this.state.pusher++;
                        this.sockets.push(pws);
                    }
                } catch (_) {}
            });

            pws.on('open', () => {
                pusherPing = setInterval(() => {
                    try {
                        if (pws.readyState === WebSocket.OPEN)
                            pws.send(JSON.stringify({ event: 'pusher:ping', data: {} }));
                    } catch (_) {}
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

    // ──────────────────────────────────────────────────────────────────────
    // refreshTokenPool — idéntico a v9.1
    // ──────────────────────────────────────────────────────────────────────
    refreshTokenPool() {
        if (this.isRefreshing) return;
        this.isRefreshing = true;

        const needed = Math.max(
            Math.ceil(this.viewerCount * 0.25),
            this.viewerCount - this.state.connected
        );
        const capped = Math.max(50, Math.min(needed, this.viewerCount));

        console.log(`[🔄] Refrescando pool: solicitando ${capped} tokens...`);

        try {
            const tokens = this.getViewerTokens(capped);
            if (tokens && tokens.length > 0) {
                this.tokenPool = this.tokenPool.concat(tokens);
                if (this.tokenPool.length > this.viewerCount * 2)
                    this.tokenPool = this.tokenPool.slice(-this.viewerCount);
                this.state.poolSize = this.tokenPool.length;
                this.state.refreshes++;
                console.log(`[✅] Pool: ${this.tokenPool.length} tokens disponibles`);
            } else {
                console.log('[⚠️] Refresh falló, reintentando en 2 min...');
            }
        } catch (e) {
            console.log(`[❌] Error en refresh: ${e.message.split('\n')[0]}`);
        }

        this.isRefreshing = false;
    }

    startTokenRefreshLoop() {
        setTimeout(() => {
            this.refreshTokenPool();
            setInterval(() => this.refreshTokenPool(), 8 * 60 * 1000);
        }, 5 * 60 * 1000);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Dashboard — idéntico a v9.1
    // ──────────────────────────────────────────────────────────────────────
    updateDashboard() {
        this.state.poolSize = this.tokenPool.length;
        console.clear();
        console.log([
            '',
            '╔══════════════════════════════════════════════════════════╗',
            '║   KICK VIEWER BOT v15.0 — Stable                      ║',
            '╠══════════════════════════════════════════════════════════╣',
            `║  Stream      : ${pad(this.streamName, 42)}║`,
            `║  Channel ID  : ${pad(this.channelId || '???', 42)}║`,
            '╠══════════════════════════════════════════════════════════╣',
            `║  Tokens JWT  : ${pad(this.state.tokens, 42)}║`,
            `║  Token Pool  : ${pad(this.tokenPool.length + ' disponibles', 42)}║`,
            `║  Refreshes   : ${pad(this.state.refreshes + ' ciclos completados', 42)}║`,
            `║  ViewerWS    : ${pad(this.state.connected + ' / ' + this.viewerCount, 42)}║`,
            `║  PusherWS    : ${pad(this.state.pusher + ' / ' + this.viewerCount, 42)}║`,
            `║  Handshakes  : ${pad(this.state.handshake, 42)}║`,
            `║  Fallidos    : ${pad(this.state.failed, 42)}║`,
            `║  Reconex.    : ${pad(this.state.reconnects, 42)}║`,
            `║  RAM total   : ${pad(this.memUsage(), 42)}║`,
            `║  Uptime      : ${pad(this.formatUptime(), 42)}║`,
            '╠══════════════════════════════════════════════════════════╣',
            `║  ${pad(
                this.state.connected > 0
                    ? `✅ ${this.state.connected} ViewerWS + ${this.state.pusher} PusherWS`
                    : this.state.tokens > 0
                        ? '⏳ Conectando viewers...'
                        : '🔑 Obteniendo tokens...',
                57)}║`,
            '╚══════════════════════════════════════════════════════════╝',
            '',
        ].join('\n'));
    }

    // ──────────────────────────────────────────────────────────────────────
    // start — idéntico a v9.1
    // ──────────────────────────────────────────────────────────────────────
    async start() {
        console.clear();
        console.log('\n╔══════════════════════════════════════════════════════════╗');
        console.log('║   KICK VIEWER BOT v15.0 — Stable                      ║');
        console.log('║   Método: curl_cffi + ViewerWS + PusherWS por viewer   ║');
        console.log('╚══════════════════════════════════════════════════════════╝\n');
        console.log(`  Stream   : ${this.streamName}`);
        console.log(`  Viewers  : ${this.viewerCount}`);

        // 1. Canal
        console.log('\n  [1/3] Obteniendo info del canal...');
        this.getChannelInfo();
        if (!this.channelId) {
            console.log('  ⚠️  No se pudo obtener channel ID');
            process.exit(1);
        }

        // 2. Tokens (método original v9.1 sin modificar)
        console.log('\n  [2/3] Obteniendo viewer tokens...');
        const tokens = this.getViewerTokens(this.viewerCount);

        if (tokens.length === 0) {
            console.error('\n  ❌ No se obtuvieron tokens.');
            console.error('     Verifica que curl_cffi está instalado: pip install curl_cffi');
            process.exit(1);
        }

        // Pre-cargar pool (25% extra — igual a v9.1)
        console.log('\n  [2.5/3] Pre-cargando token pool...');
        const extraCount  = Math.min(Math.ceil(this.viewerCount * 0.25), 500);
        const extraTokens = this.getViewerTokens(extraCount);
        if (extraTokens.length > 0) {
            this.tokenPool = extraTokens;
            console.log(`  ✅ Token pool: ${this.tokenPool.length} tokens de reserva`);
        }

        // 3. Conectar viewers
        console.log(`\n  [3/3] Conectando ${tokens.length} viewers (x2 WS cada uno)...\n`);
        for (let i = 0; i < tokens.length; i++) {
            this.connectViewer(i + 1, tokens[i]);
            const stagger = tokens.length > 1000
                ? 50  + Math.random() * 100
                : 250 + Math.random() * 350;
            await new Promise(r => setTimeout(r, stagger));
        }

        this.startTokenRefreshLoop();
        setInterval(() => this.updateDashboard(), 10000);

        console.log(`\n  ✅ ${tokens.length} viewers lanzados. Dashboard en 10s...`);
        console.log('  🔄 Auto-refresh de tokens activado (cada 8 min)');
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────
const targetUrl   = process.argv[2];
const targetCount = parseInt(process.argv[3]) || 50;

if (!targetUrl) {
    console.error('\nUSO: node kick-websocket-v14.js <URL> [viewers]');
    console.error('EJ : node kick-websocket-v14.js "https://kick.com/canal" 100\n');
    console.error('REQUISITOS: pip install curl_cffi && npm install ws');
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