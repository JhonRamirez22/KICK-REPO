#!/usr/bin/env python3
"""
Proxy Finder — Busca proxies SOCKS5/HTTP funcionando con Kick.com
"""
import requests, json, sys, threading, time

PROXY_SOURCES = [
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
    "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt",
]

WORKING = []
LOCK = threading.Lock()
CHECKED = 0

def test_proxy(proxy):
    global CHECKED
    proxy = proxy.strip()
    if not proxy: return
    if not proxy.startswith("socks"): proxy = f"socks5://{proxy}"
    with LOCK: CHECKED += 1
    try:
        r = requests.get(
            "https://kick.com/api/v1/channels/tanizen",
            proxies={"http": proxy, "https": proxy},
            headers={"User-Agent": "Mozilla/5.0 Chrome/131", "Accept": "application/json"},
            timeout=10
        )
        if r.status_code == 200:
            with LOCK:
                WORKING.append(proxy)
                print(f"[✓] {proxy[:50]}... HTTP 200", flush=True)
        elif r.status_code == 403:
            pass  # bloqueado
        else:
            with LOCK:
                print(f"[{r.status_code}] {proxy[:50]}...", flush=True)
    except:
        pass

def main():
    proxies = set()
    for url in PROXY_SOURCES:
        try:
            r = requests.get(url, timeout=15)
            for line in r.text.strip().split("\n"):
                p = line.strip()
                if p and ":" in p:
                    proxies.add(p)
        except:
            pass

    print(f"Testing {len(proxies)} proxies...", flush=True)
    threads = []
    for p in list(proxies)[:500]:  # max 500
        t = threading.Thread(target=test_proxy, args=(p,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=30)

    print(f"\nChecked: {CHECKED} | Working: {len(WORKING)}", flush=True)
    with open("proxies.txt", "w") as f:
        for p in WORKING:
            f.write(p + "\n")

    print(f"Saved {len(WORKING)} working proxies to proxies.txt", flush=True)
    return len(WORKING)

if __name__ == "__main__":
    count = main()
    sys.exit(0 if count > 0 else 1)
