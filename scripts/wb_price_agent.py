"""
WB Price Agent — запускается на ноуте (Windows), слушает HTTP на порту 8100.
Сервер обращается: GET http://100.65.13.99:8100/prices?nm=123456;789012

Запуск:  python wb_price_agent.py
"""

import asyncio
import json
import subprocess
import sys
import time
import urllib.request
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# ── Конфиг ──────────────────────────────────────────────────────────────
PORT        = 8100
CDP_PORT    = 9224
PROFILE_DIR = r"C:\ozon-collector\profile-wb"
CHROME_EXE  = r"C:\ozon-collector\chrome-win\chrome.exe"
WB_START_URL = "https://www.wildberries.ru"
LOG_FILE    = r"C:\ozon-collector\log-wb-agent.log"

WB_API = (
    "https://www.wildberries.ru/__internal/u-card/cards/v4/detail"
    "?appType=1&curr=rub&dest=-1257786&spp=30"
    "&hide_vflags=4294967296&hide_dtype=15&lang=ru&ab_testing=false"
)

# ── Логирование ──────────────────────────────────────────────────────────

class DirectHandler(logging.Handler):
    def emit(self, record):
        line = self.format(record) + "\n"
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass
        try:
            print(line, end="", flush=True)
        except Exception:
            pass

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[DirectHandler()],
    force=True,
)
log = logging.getLogger(__name__)
# websockets debug слишком шумный — глушим
logging.getLogger("websockets").setLevel(logging.WARNING)

# ── Chrome управление ────────────────────────────────────────────────────

def is_chrome_running() -> bool:
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/version", timeout=2)
        return True
    except Exception:
        return False


def start_chrome():
    if is_chrome_running():
        log.info("Chrome уже запущен на порту %d", CDP_PORT)
        return
    log.info("Запускаю Chrome: %s", CHROME_EXE)
    subprocess.Popen([
        CHROME_EXE,
        f"--remote-debugging-port={CDP_PORT}",
        f"--user-data-dir={PROFILE_DIR}",
        "--remote-allow-origins=*",
        "--no-first-run",
        "--disable-background-timer-throttling",
        WB_START_URL,
    ])
    for i in range(20):
        time.sleep(1)
        if is_chrome_running():
            log.info("Chrome запущен (через %d сек)", i + 1)
            return
    log.error("Chrome не запустился за 20 секунд")


def ensure_wb_tab() -> str | None:
    log.debug("ensure_wb_tab: запрашиваю /json/list...")
    try:
        raw = urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/list", timeout=5).read()
        tabs = json.loads(raw)
        log.debug("  всего вкладок: %d", len(tabs))
        wb = [t for t in tabs if "wildberries.ru" in t.get("url", "") and t.get("type") == "page"]
        log.debug("  WB-вкладок: %d", len(wb))
        if wb:
            url = wb[0]["webSocketDebuggerUrl"]
            log.debug("  ws_url: %s", url)
            return url
        log.info("Открываю WB-вкладку...")
        urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/new?{WB_START_URL}", timeout=5)
        time.sleep(3)
        raw = urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/list", timeout=5).read()
        tabs = json.loads(raw)
        wb = [t for t in tabs if "wildberries.ru" in t.get("url", "") and t.get("type") == "page"]
        if wb:
            log.debug("  ws_url (новая вкладка): %s", wb[0]["webSocketDebuggerUrl"])
            return wb[0]["webSocketDebuggerUrl"]
        log.error("  WB-вкладка не найдена после открытия")
        return None
    except Exception as e:
        log.error("ensure_wb_tab exception: %s: %s", type(e).__name__, e)
        return None


# ── WB fetch через CDP ────────────────────────────────────────────────────

async def _fetch_async(ws_url: str, nm_str: str) -> list[dict]:
    import websockets
    js = (
        f'fetch("{WB_API}&nm={nm_str}",'
        '{headers:{"Accept":"*/*","Accept-Language":"ru-RU,ru;q=0.9"},credentials:"include"})'
        ".then(r=>r.json()).then(d=>JSON.stringify(d))"
    )
    log.debug("  CDP connect: %s", ws_url)
    async with websockets.connect(ws_url, max_size=20 * 1024 * 1024) as ws:
        log.debug("  CDP connected, отправляю Runtime.evaluate...")
        await ws.send(json.dumps({
            "id": 1, "method": "Runtime.evaluate",
            "params": {"expression": js, "awaitPromise": True, "returnByValue": True}
        }))
        log.debug("  ожидаю ответ WB API (до 20 сек)...")
        resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
        val = resp.get("result", {}).get("result", {}).get("value", "")
        if not val:
            log.error("CDP пустой ответ: %s", str(resp)[:300])
            return []
        products = json.loads(val).get("products", [])
        log.debug("  WB API вернул %d товаров", len(products))
        return products


_cdp_loop: asyncio.AbstractEventLoop | None = None


def fetch_prices_cdp(nm_ids: list[int]) -> list[dict]:
    global _cdp_loop
    log.debug("fetch_prices_cdp: nm_ids=%s, loop=%s", nm_ids, _cdp_loop)
    ws_url = ensure_wb_tab()
    if not ws_url:
        log.error("Нет WB-вкладки")
        return []
    nm_str = ";".join(str(n) for n in nm_ids)
    log.debug("Запускаю _fetch_async через _cdp_loop.run_until_complete")
    try:
        result = _cdp_loop.run_until_complete(_fetch_async(ws_url, nm_str))
        log.debug("run_until_complete вернул %d товаров", len(result))
        return result
    except Exception as e:
        log.error("CDP fetch error: %s: %s", type(e).__name__, e)
        return []


def parse_products(products: list[dict]) -> list[dict]:
    result = []
    for p in products:
        sizes = p.get("sizes", [])
        price = price_basic = stock = 0
        for s in sizes:
            pr = s.get("price", {})
            if pr.get("product"):
                price = pr["product"] // 100
                price_basic = pr.get("basic", 0) // 100
            stock += sum(st.get("qty", 0) for st in s.get("stocks", []))
        result.append({
            "id":          p.get("id"),
            "name":        p.get("name", ""),
            "brand":       p.get("brand", ""),
            "seller":      p.get("supplier", ""),
            "price":       price,
            "price_basic": price_basic,
            "rating":      p.get("rating", 0),
            "feedbacks":   p.get("feedbacks", 0),
            "stock":       stock,
        })
    return result


# ── HTTP сервер ───────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log.debug("HTTP %s %s", self.command, self.path)

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            self._json({"ok": True, "chrome": is_chrome_running()})
            return

        if parsed.path == "/prices":
            params = parse_qs(parsed.query)
            nm_raw = (params.get("nm") or [""])[0]
            nm_ids = []
            for x in nm_raw.replace(",", ";").split(";"):
                x = x.strip()
                if x.isdigit():
                    nm_ids.append(int(x))

            if not nm_ids:
                self._json({"error": "nm parameter required", "products": []}, 400)
                return

            if not is_chrome_running():
                log.warning("Chrome не запущен, запускаю...")
                start_chrome()

            log.info(">>> Запрос цен: %d товаров %s", len(nm_ids), nm_ids)
            products = fetch_prices_cdp(nm_ids)
            result = parse_products(products)
            log.info("<<< Получено: %d товаров", len(result))
            for r in result:
                log.info("    %s | %s | %d₽ | stock=%d", r["id"], r["name"][:40], r["price"], r["stock"])
            self._json({"products": result})
            return

        self._json({"error": "not found"}, 404)

    def _json(self, data: dict, status: int = 200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


# ── Запуск ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("WB Price Agent запускается на порту %d", PORT)
    log.info("Python: %s", sys.executable)
    log.info("Chrome CDP порт: %d", CDP_PORT)
    log.info("=" * 60)

    start_chrome()

    # Event loop создаём ОДИН раз и используем для всех запросов
    log.info("Создаю asyncio event loop...")
    _cdp_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_cdp_loop)
    log.info("Event loop: %s", type(_cdp_loop).__name__)

    # Стартовый тест
    log.info("--- Стартовый тест CDP fetch ---")
    _ws = ensure_wb_tab()
    if _ws:
        try:
            _prods = _cdp_loop.run_until_complete(_fetch_async(_ws, "542415681"))
            log.info("Стартовый тест: %d товаров — %s", len(_prods), "OK" if _prods else "ПУСТО")
        except Exception as _e:
            log.error("Стартовый тест ОШИБКА: %s: %s", type(_e).__name__, _e)
    else:
        log.error("Стартовый тест: нет WB-вкладки")
    log.info("--- Стартовый тест завершён ---")

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    log.info("Слушаю http://0.0.0.0:%d  (Ctrl+C для остановки)", PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Остановка")
    finally:
        _cdp_loop.close()
