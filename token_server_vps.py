#!/usr/bin/env python3
"""
Token Server v17.4 — Solo Tor proxy con timeouts altos
"""
import json, uuid, asyncio, logging, os, time
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
from curl_cffi import requests as cffi_requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TokenServer")

app = FastAPI(title="Kick Token Server", version="17.4")

CLIENT_TOKEN = os.environ.get("CLIENT_TOKEN", "e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823")
CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
PROXIES = {"http": "socks5://127.0.0.1:9050", "https": "socks5://127.0.0.1:9050"}

async def fetch_token(channel_name, retries=10):
    for attempt in range(retries):
        try:
            session = cffi_requests.Session(impersonate="chrome131", proxies=PROXIES)

            session.get(f"https://kick.com/{channel_name}", headers={
                "User-Agent": CHROME_UA, "Accept": "text/html,*/*"}, timeout=60)

            res = session.get("https://websockets.kick.com/viewer/v1/token", headers={
                "User-Agent": CHROME_UA, "Accept": "application/json",
                "Origin": "https://kick.com", "Referer": f"https://kick.com/{channel_name}",
                "X-CLIENT-TOKEN": CLIENT_TOKEN,
                "X-Device-ID": str(uuid.uuid4()),
                "X-Session-ID": str(uuid.uuid4()),
            }, timeout=60)

            if res.status_code == 200:
                token = res.json().get("data", {}).get("token", "")
                if token and len(token) > 20:
                    return token
            elif res.status_code == 429:
                logger.warning(f"Rate limit ({attempt+1})")
                await asyncio.sleep(10)
            else:
                logger.warning(f"HTTP {res.status_code} ({attempt+1})")
                await asyncio.sleep(5)
        except Exception as e:
            logger.warning(f"({attempt+1}) {str(e)[:70]}")
            await asyncio.sleep(8)
    return None

async def get_channel_info(channel_name):
    for attempt in range(3):
        try:
            res = cffi_requests.get(f"https://kick.com/api/v1/channels/{channel_name}",
                impersonate="chrome131", proxies=PROXIES,
                headers={"User-Agent": CHROME_UA, "Accept": "application/json"}, timeout=60)
            if res.status_code == 200:
                d = res.json()
                ls = d.get("livestream") or {}
                return {"id": d.get("id"), "chatroom_id": (d.get("chatroom") or {}).get("id"),
                        "live": d.get("livestream") is not None,
                        "viewers": ls.get("viewers", 0), "title": ls.get("session_title", "")}
            return {"error": f"HTTP {res.status_code}"}
        except Exception as e:
            logger.warning(f"Info channel ({attempt+1}): {str(e)[:60]}")
            await asyncio.sleep(5)
    return {"error": "all attempts failed"}

@app.get("/batch-tokens")
async def batch_tokens(channel: str, count: int = 10):
    if count > 30: count = 30
    sem = asyncio.Semaphore(3)
    async def limited():
        async with sem:
            return await fetch_token(channel)
    tasks = [limited() for _ in range(count)]
    results = await asyncio.gather(*tasks)
    valid = [t for t in results if isinstance(t, str) and len(t) > 20]
    logger.info(f"Batch: {len(valid)}/{count}")
    return {"tokens": valid, "count": len(valid)}

@app.get("/token")
async def token(channel: str):
    t = await fetch_token(channel)
    if t: return {"token": t}
    return JSONResponse(status_code=503, content={"error": "No token"})

@app.get("/discover")
async def discover(channel: str):
    info = await get_channel_info(channel)
    if info.get("error"):
        return JSONResponse(status_code=404, content=info)
    return info

@app.get("/health")
async def health():
    return {"status": "ok", "mode": "tor"}

if __name__ == "__main__":
    logger.info("Iniciando Token Server...")
    try:
        uvicorn.run(app, host="127.0.0.1", port=8765, log_level="info")
    except Exception as e:
        logger.error(f"Error iniciando server: {e}")
        time.sleep(3)
        uvicorn.run(app, host="127.0.0.1", port=8765, log_level="info")
