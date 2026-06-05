"""
Тестовый скрипт — запускается вручную, проверяет всю цепочку.

Запуск:  python wb_price_test.py
Лог:     C:\ozon-collector\test-output.txt  (и в окне)
"""

import asyncio
import json
import subprocess
import sys
import time
import urllib.request
import logging

CDP_PORT    = 9224
CHROME_EXE  = r"C:\ozon-collector\chrome-win\chrome.exe"
PROFILE_DIR = r"C:\ozon-collector\profile-wb"
WB_START_URL = "https://www.wildberries.ru"
LOG_FILE    = r"C:\ozon-collector\test-output.txt"

WB_API = (
    "https://www.wildberries.ru/__internal/u-card/cards/v4/detail"
    "?appType=1&curr=rub&dest=-1257786&spp=30"
    "&hide_vflags=4294967296&hide_dtype=15&lang=ru&ab_testing=false"
)

# Тестовые артикулы (реальные)
TEST_NM_IDS = [542415681, 542415533, 542415532]

# ── Лог ─────────────────────────────────────────────────────────────────

class DirectHandler(logging.Handler):
    def __init__(self, path):
        super().__init__()
        self._path = path
        # Создаём/очищаем файл
        with open(path, "w", encoding="utf-8") as f:
            f.write("=== WB Price Test ===\n")

    def emit(self, record):
        line = self.format(record) + "\n"
        try:
            with open(self._path, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass
        try:
            print(line, end="", flush=True)
        except Exception:
            pass

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s",
    handlers=[DirectHandler(LOG_FILE)])
log = logging.getLogger(__name__)


# ── Chrome ───────────────────────────────────────────────────────────────

def ensure_chrome():
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/version", timeout=2)
        log.info("Chrome уже запущен")
        return
    except Exception:
        pass
    log.info(f"Запускаю Chrome: {CHROME_EXE}")
    subprocess.Popen([
        CHROME_EXE,
        f"--remote-debugging-port={CDP_PORT}",
        f"--user-data-dir={PROFILE_DIR}",
        "--remote-allow-origins=*",
        "--no-first-run",
        WB_START_URL,
    ])
    for i in range(20):
        time.sleep(1)
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/version", timeout=2)
            log.info(f"Chrome запустился (через {i+1} сек)")
            time.sleep(2)  # ждём загрузку WB
            return
        except Exception:
            pass
    log.error("Chrome не запустился за 20 секунд")


# ── CDP ───────────────────────────────────────────────────────────────────

def get_wb_tab_url() -> str | None:
    log.info(f"Запрашиваю список вкладок CDP на порту {CDP_PORT}...")
    try:
        raw = urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/list", timeout=5).read()
        tabs = json.loads(raw)
        log.info(f"  Всего вкладок: {len(tabs)}")
        for t in tabs:
            log.debug(f"    {t.get('type'):10} {t.get('url','')[:70]}")
        wb = [t for t in tabs if "wildberries.ru" in t.get("url", "") and t.get("type") == "page"]
        log.info(f"  WB page вкладок: {len(wb)}")
        if not wb:
            log.error("Нет WB-вкладок! Открой wildberries.ru в Chrome с CDP-профилем")
            return None
        url = wb[0]["webSocketDebuggerUrl"]
        log.info(f"  Используем: {url}")
        return url
    except Exception as e:
        log.error(f"CDP /json/list ошибка: {e}")
        return None


async def fetch_nm_ids(ws_url: str, nm_ids: list[int]) -> list[dict]:
    nm_str = ";".join(str(n) for n in nm_ids)
    js = (
        f'fetch("{WB_API}&nm={nm_str}",'
        '{headers:{"Accept":"*/*","Accept-Language":"ru-RU,ru;q=0.9"},credentials:"include"})'
        ".then(r=>r.json()).then(d=>JSON.stringify(d))"
    )

    log.info(f"Подключаюсь к WebSocket CDP...")
    try:
        import websockets
    except ImportError:
        log.error("websockets не установлен: pip install websockets")
        return []

    try:
        async with websockets.connect(ws_url, max_size=20 * 1024 * 1024) as ws:
            log.info("Подключился. Отправляю Runtime.evaluate...")
            await ws.send(json.dumps({
                "id": 1,
                "method": "Runtime.evaluate",
                "params": {"expression": js, "awaitPromise": True, "returnByValue": True}
            }))
            log.info("Жду ответ (до 20 сек)...")
            resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
            val = resp.get("result", {}).get("result", {}).get("value", "")
            if not val:
                log.error(f"Пустой ответ от CDP. resp={str(resp)[:300]}")
                return []
            products = json.loads(val).get("products", [])
            log.info(f"Получено {len(products)} товаров от WB API")
            return products
    except Exception as e:
        log.error(f"WebSocket ошибка: {e}")
        return []


def parse_price(products: list[dict]) -> list[dict]:
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
            "id": p.get("id"),
            "name": p.get("name", ""),
            "price": price,
            "price_basic": price_basic,
            "stock": stock,
            "seller": p.get("supplier", ""),
        })
    return result


async def main():
    log.info("=" * 60)
    log.info(f"Python: {sys.executable}")
    log.info(f"Тестируем {len(TEST_NM_IDS)} артикулов: {TEST_NM_IDS}")
    log.info("=" * 60)

    ensure_chrome()

    ws_url = get_wb_tab_url()
    if not ws_url:
        log.error("Завершаю — нет WB-вкладки")
        return

    log.info("")
    products = await fetch_nm_ids(ws_url, TEST_NM_IDS)
    parsed = parse_price(products)

    log.info("")
    log.info("── РЕЗУЛЬТАТ ─────────────────────────────────────────────")
    if parsed:
        for p in parsed:
            log.info(f"  {p['id']} | {p['name'][:45]} | {p['price']:,}₽ | stock={p['stock']}")
    else:
        log.warning("  Товары не получены")
    log.info("=" * 60)


asyncio.run(main())
