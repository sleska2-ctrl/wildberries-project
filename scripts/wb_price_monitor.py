"""
WB Price Monitor — запускается с сервера, парсит цены через браузер на ноуте.

Архитектура:
  1. Скрипт SSH-тоннелит CDP-порт ноута (9222 → localhost:19222)
  2. Через браузер делает fetch() запросы к WB с реальными куками
  3. Пакетно (до 50 nmId за раз) — ~2400 товаров за 50 запросов
  4. Пишет историю цен в SQLite: data/wb_prices.db

Запуск: python3 scripts/wb_price_monitor.py [--cabinet ewb] [--dry-run]
Cron:   0 8,20 * * * cd /opt/wildberries/app && python3 scripts/wb_price_monitor.py >> logs/price_monitor.log 2>&1
"""

import asyncio
import json
import sqlite3
import argparse
import subprocess
import time
import os
import sys
from datetime import datetime, date

# ── Конфиг ──────────────────────────────────────────────────────────────
LAPTOP_SSH    = "ии@100.65.13.99"
LAPTOP_CDP    = 9224
LOCAL_CDP     = 19224
# HTTP-агент на ноуте (wb_price_agent.py) — приоритетный способ
AGENT_URL     = "http://100.65.13.99:8100"
BATCH_SIZE    = 50
DELAY_SEC     = 1.5
PRICES_DB    = os.path.join(os.path.dirname(__file__), "../data/wb_prices.db")
PLATFORM_DB  = os.path.join(os.path.dirname(__file__), "../data/platform.db")
COMPETITOR_CABINET = "hld"  # только для этого кабинета собираем цены конкурентов

WB_URL = (
    "https://www.wildberries.ru/__internal/u-card/cards/v4/detail"
    "?appType=1&curr=rub&dest=-1257786&spp=30"
    "&hide_vflags=4294967296&hide_dtype=15&lang=ru&ab_testing=false"
)

# ── DB ──────────────────────────────────────────────────────────────────
def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            date        TEXT NOT NULL,
            cabinet_id  TEXT NOT NULL,
            nm_id       INTEGER NOT NULL,
            name        TEXT,
            brand       TEXT,
            seller      TEXT,
            price       INTEGER,
            price_basic INTEGER,
            rating      REAL,
            feedbacks   INTEGER,
            stock       INTEGER
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ph_nm_date ON price_history(nm_id, date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ph_cab_date ON price_history(cabinet_id, date)")
    conn.commit()
    return conn


def get_nm_ids(cabinet_id: str | None, limit: int | None = None) -> list[tuple[str, int]]:
    """
    Возвращает [(cabinet_id, nm_id), ...] товаров в наличии из raw_stocks.
    Берём только nmId с ненулевым остатком на последнюю дату синка.
    """
    result = []
    try:
        pconn = sqlite3.connect(PLATFORM_DB)
        cabs = pconn.execute(
            "SELECT cabinet_id FROM cabinets WHERE marketplace IN ('wb','both')"
            + (f" AND cabinet_id='{cabinet_id}'" if cabinet_id else "")
        ).fetchall()
        pconn.close()

        for (cab_id,) in cabs:
            cab_db = os.path.join(os.path.dirname(__file__), f"../data/cabs/{cab_id}.db")
            if not os.path.exists(cab_db):
                continue
            cconn = sqlite3.connect(cab_db)
            try:
                last_date = cconn.execute(
                    "SELECT MAX(lastChangeDate) FROM raw_stocks"
                ).fetchone()[0]
                if not last_date:
                    cconn.close()
                    continue
                rows = cconn.execute("""
                    SELECT nmId
                    FROM raw_stocks
                    WHERE DATE(lastChangeDate) = DATE(?)
                    GROUP BY nmId
                    HAVING SUM(CAST(quantity AS INTEGER)) + SUM(CAST(inWayToClient AS INTEGER)) > 0
                    ORDER BY SUM(CAST(quantity AS INTEGER)) DESC
                """, (last_date,)).fetchall()
                for (nm,) in rows:
                    try:
                        result.append((cab_id, int(str(nm).strip())))
                    except (ValueError, TypeError):
                        pass
            except sqlite3.OperationalError as e:
                print(f"[WARN] {cab_id}: {e}", file=sys.stderr)
            finally:
                cconn.close()
    except Exception as e:
        print(f"[WARN] get_nm_ids: {e}", file=sys.stderr)
    return result


# ── Fetch через HTTP-агент (приоритет) или CDP-туннель (fallback) ────────

def _fetch_via_agent(nm_ids: list[int]) -> list[dict] | None:
    """
    Запрашивает цены через wb_price_agent.py на ноуте.
    GET http://100.65.13.99:8100/prices?nm=123;456;789
    Возвращает список продуктов или None если агент недоступен.
    """
    import urllib.request
    nm_str = ";".join(str(n) for n in nm_ids)
    url = f"{AGENT_URL}/prices?nm={nm_str}"
    try:
        r = urllib.request.urlopen(url, timeout=30)
        d = json.loads(r.read())
        return d.get("products", [])
    except Exception as e:
        print(f"  [WARN] агент недоступен: {e}", file=sys.stderr)
        return None


async def _fetch_via_cdp_tunnel(nm_ids: list[int]) -> list[dict]:
    """Fallback: CDP через SSH-туннель."""
    try:
        import websockets
    except ImportError:
        return []

    import urllib.request
    try:
        tabs_raw = urllib.request.urlopen(f"http://127.0.0.1:{LOCAL_CDP}/json/list", timeout=5).read()
        tabs = json.loads(tabs_raw)
        wb = [t for t in tabs if "wildberries" in t.get("url", "") and t.get("type") == "page"]
        if not wb:
            # Открываем WB-вкладку
            urllib.request.urlopen(
                f"http://127.0.0.1:{LOCAL_CDP}/json/new?https://www.wildberries.ru", timeout=5
            )
            await asyncio.sleep(3)
            tabs_raw = urllib.request.urlopen(f"http://127.0.0.1:{LOCAL_CDP}/json/list", timeout=5).read()
            tabs = json.loads(tabs_raw)
            wb = [t for t in tabs if "wildberries" in t.get("url", "") and t.get("type") == "page"]
        if not wb:
            print("[ERR] Нет WB-вкладки", file=sys.stderr)
            return []
        ws_url = wb[0]["webSocketDebuggerUrl"]
    except Exception as e:
        print(f"[ERR] CDP туннель недоступен: {e}", file=sys.stderr)
        return []

    nm_str = ";".join(str(n) for n in nm_ids)
    js = (
        f'fetch("{WB_URL}&nm={nm_str}",'
        '{{headers:{{"Accept":"*/*","Accept-Language":"ru-RU,ru;q=0.9"}},'
        'credentials:"include"}})'
        '.then(r=>r.json()).then(d=>JSON.stringify(d))'
    )
    try:
        async with websockets.connect(ws_url, max_size=20 * 1024 * 1024) as ws:
            await ws.send(json.dumps({
                "id": 1, "method": "Runtime.evaluate",
                "params": {"expression": js, "awaitPromise": True, "returnByValue": True}
            }))
            resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
            val = resp.get("result", {}).get("result", {}).get("value", "")
            if not val:
                return []
            d = json.loads(val)
            return d.get("products", [])
    except Exception as e:
        print(f"[ERR] CDP fetch: {e}", file=sys.stderr)
        return []


async def fetch_prices_via_browser(nm_ids: list[int]) -> list[dict]:
    """Сначала пробует HTTP-агент, при недоступности — CDP-туннель."""
    result = _fetch_via_agent(nm_ids)
    if result is not None:
        return result
    # Fallback на CDP
    return await _fetch_via_cdp_tunnel(nm_ids)


def parse_product(p: dict) -> dict:
    sizes = p.get("sizes", [])
    price = 0
    price_basic = 0
    stock = 0
    for s in sizes:
        pr = s.get("price", {})
        if pr.get("product"):
            price = pr["product"] // 100
            price_basic = pr.get("basic", 0) // 100
        stocks = s.get("stocks", [])
        stock += sum(st.get("qty", 0) for st in stocks)

    return {
        "nm_id":       p.get("id"),
        "name":        p.get("name", ""),
        "brand":       p.get("brand", ""),
        "seller":      p.get("supplier", ""),
        "price":       price,
        "price_basic": price_basic,
        "rating":      p.get("rating", 0),
        "feedbacks":   p.get("feedbacks", 0),
        "stock":       stock,
    }


# ── SSH туннель ──────────────────────────────────────────────────────────
def ensure_tunnel() -> subprocess.Popen | None:
    """Открывает SSH-туннель если ещё не открыт."""
    import socket
    sock = socket.socket()
    try:
        sock.connect(("127.0.0.1", LOCAL_CDP))
        sock.close()
        return None  # уже открыт
    except ConnectionRefusedError:
        pass
    finally:
        sock.close()

    proc = subprocess.Popen([
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-f", "-N",
        "-L", f"{LOCAL_CDP}:127.0.0.1:{LAPTOP_CDP}",
        LAPTOP_SSH
    ])
    time.sleep(2)
    return proc


# ── Конкуренты ────────────────────────────────────────────────────────────
def get_competitor_nm_ids() -> dict[int, int]:
    """
    Возвращает {comp_nm_id: our_nm_id} из competitor_products для hld.
    Используется для сбора цен конкурентов.
    """
    if not os.path.exists(PRICES_DB):
        return {}
    try:
        conn = sqlite3.connect(PRICES_DB)
        rows = conn.execute(
            "SELECT DISTINCT comp_nm_id, our_nm_id FROM competitor_products WHERE cabinet_id=?",
            (COMPETITOR_CABINET,)
        ).fetchall()
        conn.close()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        print(f"[WARN] get_competitor_nm_ids: {e}", file=sys.stderr)
        return {}


def save_competitor_prices(conn: sqlite3.Connection, ts: str, today: str,
                           products: list[dict], comp_to_our: dict[int, int]) -> int:
    """Сохраняет цены конкурентов в competitor_prices."""
    rows = []
    for p in products:
        parsed = parse_product(p)
        comp_nm = parsed["nm_id"]
        our_nm  = comp_to_our.get(comp_nm)
        if not our_nm:
            continue
        if parsed["price"] == 0 and parsed["stock"] == 0:
            continue
        rows.append((
            ts, today, COMPETITOR_CABINET, our_nm, comp_nm,
            parsed["price"], parsed["price_basic"],
            parsed["stock"], parsed["rating"], parsed["feedbacks"]
        ))
    if rows:
        conn.executemany("""
            INSERT INTO competitor_prices
              (ts, date, cabinet_id, our_nm_id, comp_nm_id,
               price, price_basic, stock, rating, feedbacks)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, rows)
        conn.commit()
    return len(rows)


# ── Main ─────────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cabinet", help="cabinet_id (по умолч. — все WB-кабинеты)")
    parser.add_argument("--dry-run", action="store_true", help="не писать в БД")
    parser.add_argument("--test", action="store_true", help="тест: только 3 товара из каждого кабинета")
    parser.add_argument("--cdp-port", type=int, default=None, help="переопределить LOCAL_CDP порт")
    args = parser.parse_args()

    global LOCAL_CDP
    if args.cdp_port:
        LOCAL_CDP = args.cdp_port

    tunnel = ensure_tunnel()

    nm_pairs = get_nm_ids(args.cabinet)
    if not nm_pairs:
        print("[WARN] Не найдено nmId — проверь SKU кабинетов")
        return

    # Группируем по cabinet_id
    from collections import defaultdict
    by_cab: dict[str, list[int]] = defaultdict(list)
    for cab_id, nm_id in nm_pairs:
        by_cab[cab_id].append(nm_id)

    test_limit = 3 if args.test else None

    conn = None if args.dry_run else init_db(PRICES_DB)
    ts = datetime.now().isoformat(timespec="seconds")
    today = date.today().isoformat()

    total_saved = 0
    total_not_found = 0

    for cab_id, nm_ids in by_cab.items():
        unique_ids = list(set(nm_ids))
        if test_limit:
            unique_ids = unique_ids[:test_limit]
        print(f"[{cab_id}] {len(unique_ids)} товаров{'  (тест)' if test_limit else ''}")

        # Батчи по BATCH_SIZE
        for i in range(0, len(unique_ids), BATCH_SIZE):
            batch = unique_ids[i:i + BATCH_SIZE]
            products = await fetch_prices_via_browser(batch)

            found_ids = {p.get("id") for p in products}
            not_found = [n for n in batch if n not in found_ids]
            if not_found:
                total_not_found += len(not_found)

            rows = []
            for p in products:
                parsed = parse_product(p)
                if parsed["price"] == 0 and parsed["stock"] == 0:
                    continue  # снят с продажи
                rows.append((
                    ts, today, cab_id,
                    parsed["nm_id"], parsed["name"], parsed["brand"], parsed["seller"],
                    parsed["price"], parsed["price_basic"],
                    parsed["rating"], parsed["feedbacks"], parsed["stock"]
                ))

            if conn and rows:
                conn.executemany("""
                    INSERT INTO price_history
                      (ts, date, cabinet_id, nm_id, name, brand, seller,
                       price, price_basic, rating, feedbacks, stock)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, rows)
                conn.commit()

            total_saved += len(rows)
            pct = min(i + BATCH_SIZE, len(unique_ids))
            print(f"  {pct}/{len(unique_ids)} | батч {i//BATCH_SIZE+1}: {len(rows)} цен")

            if i + BATCH_SIZE < len(unique_ids):
                await asyncio.sleep(DELAY_SEC)

    # ── Цены конкурентов (только для hld) ──────────────────────────────────
    comp_to_our = get_competitor_nm_ids()
    if comp_to_our and not args.dry_run:
        comp_ids = list(comp_to_our.keys())
        print(f"\n[конкуренты hld] {len(comp_ids)} артикулов")
        comp_saved = 0
        for i in range(0, len(comp_ids), BATCH_SIZE):
            batch = comp_ids[i:i + BATCH_SIZE]
            products = await fetch_prices_via_browser(batch)
            if conn:
                saved = save_competitor_prices(conn, ts, today, products, comp_to_our)
                comp_saved += saved
            pct = min(i + BATCH_SIZE, len(comp_ids))
            print(f"  {pct}/{len(comp_ids)} | батч {i//BATCH_SIZE+1}: {len(products)} получено")
            if i + BATCH_SIZE < len(comp_ids):
                await asyncio.sleep(DELAY_SEC)
        print(f"  Цен конкурентов сохранено: {comp_saved}")

    if conn:
        conn.close()

    print(f"\nГотово: сохранено {total_saved}, не найдено {total_not_found}")
    if tunnel:
        tunnel.terminate()


if __name__ == "__main__":
    asyncio.run(main())
