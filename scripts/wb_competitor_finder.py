"""
WB Competitor Finder — находит конкурентов для товаров кабинета hld через MPSTATS.

Данные о наших товарах берём из raw_stocks + raw_sales (без MPSTATS-вызовов на info):
  - raw_stocks: nmId, brand, subject (name), supplierArticle, остатки
  - raw_sales.title: полное название товара (содержит модельный номер)

Алгоритм для каждого nmId в наличии:
  1. raw_stocks → brand, subject_name; raw_sales → title (полное название)
  2. Извлекаем модельный номер из title
  3. subject_name → subject_id: один вызов MPSTATS /full для первого nmId каждой ниши
  4. MPSTATS POST /wb/subject/items?path={subject_id} + name filter → конкуренты всех брендов
  5. Сохраняем в competitor_products (wb_prices.db)

Запуск:
  python3 scripts/wb_competitor_finder.py --test   # 2 артикула
  python3 scripts/wb_competitor_finder.py          # все в наличии
"""

import argparse
import json
import re
import sqlite3
import sys
import time
import urllib.request
import urllib.parse
import os
from datetime import date, timedelta

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PRICES_DB    = os.path.join(BASE_DIR, "../data/wb_prices.db")
MPSTATS_CONF = "/root/.claude/skills/mpstats/config/.env"
MPSTATS_BASE = "https://mpstats.io/api/analytics/v1/wb"
CABINET_ID   = "hld"

# Слова-описания товаров — не являются моделью
SKIP_WORDS = {
    "встраиваемая","встраиваемый","встраиваемое",
    "газовая","газовый","электрическая","электрический","индукционная","независимая",
    "варочная","панель","плита","поверхность","конфорки","конфорка",
    "холодильник","морозильник","морозильная","камера",
    "стиральная","машина","посудомоечная","духовой","шкаф",
    "микроволновая","печь","кондиционер","телевизор","пылесос",
    "утюг","чайник","кофемашина","блендер","мультиварка",
    "двухкамерный","однокамерный","полноразмерный","узкая","узкий",
    "no","frost","nofrost","inverter","inox",
    "led","oled","qled","smart","tv","uhd","fhd","hd",
    "автомат","фронтальная","загрузка","встроенный",
    "черный","белый","серебристый","нержавеющая","сталь","серый","серебристый",
    "cis","ru","eu","sl","wh","bk","ss","akg","l",
    "essential","ecotime","eco",
    "конфорок","кон","конф",
}


def load_mpstats_token() -> str:
    try:
        with open(MPSTATS_CONF) as f:
            for line in f:
                if line.startswith("MPSTATS_TOKEN="):
                    return line.strip().split("=", 1)[1]
    except FileNotFoundError:
        pass
    token = os.environ.get("MPSTATS_TOKEN", "")
    if not token:
        sys.exit("[ERR] MPSTATS_TOKEN не найден")
    return token


def mpstats_get(token: str, path: str) -> dict:
    url = f"{MPSTATS_BASE}/{path}"
    req = urllib.request.Request(url, headers={"X-Mpstats-TOKEN": token, "Accept": "application/json"})
    try:
        r = urllib.request.urlopen(req, timeout=20)
        return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="ignore")[:200]
        print(f"  [WARN] HTTP {e.code} GET {path}: {body}", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"  [WARN] GET {path}: {e}", file=sys.stderr)
        return {}


def mpstats_post(token: str, path: str, params: dict, body: dict) -> dict:
    url = f"{MPSTATS_BASE}/{path}?" + urllib.parse.urlencode(params)
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"X-Mpstats-TOKEN": token,
                                          "Content-Type": "application/json",
                                          "Accept": "application/json"})
    try:
        r = urllib.request.urlopen(req, timeout=30)
        return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode(errors="ignore")[:300]
        print(f"  [WARN] HTTP {e.code} POST {path}: {body_text}", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"  [WARN] POST {path}: {e}", file=sys.stderr)
        return {}


def get_nm_ids_in_stock(cab_db: str, limit: int | None = None) -> list[dict]:
    """
    Возвращает список товаров в наличии из raw_stocks.
    Полные названия и subject_id берёт из wb_cards (загружается при синхронизации).
    Если wb_cards нет — fallback на raw_sales.title.
    """
    conn = sqlite3.connect(cab_db)
    conn.row_factory = sqlite3.Row

    # Последняя дата остатков
    last_date = conn.execute("SELECT MAX(lastChangeDate) FROM raw_stocks").fetchone()[0]
    if not last_date:
        conn.close()
        return []

    # Товары с ненулевым остатком
    stocks = conn.execute("""
        SELECT nmId, supplierArticle, brand, subject,
               SUM(CAST(quantity AS INTEGER)) + SUM(CAST(inWayToClient AS INTEGER)) as total
        FROM raw_stocks
        WHERE DATE(lastChangeDate) = DATE(?)
        GROUP BY nmId
        HAVING total > 0
        ORDER BY total DESC
    """, (last_date,)).fetchall()

    # Полные названия и subject_id из wb_cards (приоритет)
    titles: dict[str, str] = {}
    subject_ids: dict[str, int] = {}
    try:
        rows = conn.execute(
            "SELECT nmID, title, subject_id FROM wb_cards WHERE nmID IS NOT NULL AND title != ''"
        ).fetchall()
        for r in rows:
            key = str(r["nmID"])
            if r["title"]:
                titles[key] = r["title"]
            if r["subject_id"]:
                try:
                    subject_ids[key] = int(r["subject_id"])
                except (TypeError, ValueError):
                    pass
    except sqlite3.OperationalError:
        # wb_cards ещё не создана — fallback на raw_sales.title
        try:
            rows = conn.execute("""
                SELECT nmId, title FROM raw_sales
                WHERE title IS NOT NULL AND title != ''
                GROUP BY nmId HAVING MAX(dateFrom)
            """).fetchall()
            for r in rows:
                if r["nmId"] and r["title"]:
                    titles[str(r["nmId"])] = r["title"]
        except sqlite3.OperationalError:
            pass

    conn.close()

    result = []
    for s in stocks:
        nm_id = str(s["nmId"]) if s["nmId"] else None
        if not nm_id:
            continue
        result.append({
            "nm_id":            int(nm_id),
            "supplier_article": s["supplierArticle"] or "",
            "brand":            s["brand"] or "",
            "subject_name":     s["subject"] or "",
            "title":            titles.get(nm_id, ""),
            "subject_id_cache": subject_ids.get(nm_id),  # из wb_cards если есть
            "stock":            s["total"],
        })

    if limit:
        result = result[:limit]

    print(f"[{CABINET_ID}] Товаров в наличии: {len(result)} (дата: {last_date[:10]})")
    return result


def extract_model(title: str, brand: str) -> str | None:
    """
    Извлекает модельный номер из полного названия товара.
    Ищет токен(ы) содержащие И буквы И цифры (кроме служебных слов).
    """
    brand_tokens = {t.lower() for t in re.split(r'[\s\-/,]', brand) if t} if brand else set()

    tokens = re.split(r'[\s,;/]+', title)
    tokens = [re.sub(r'[()[\]{}<>]', '', t).strip() for t in tokens]
    tokens = [t for t in tokens if t]

    candidates = []
    for t in tokens:
        t_lower = t.lower()
        if len(t) < 2:
            continue
        if t_lower in SKIP_WORDS or t_lower in brand_tokens:
            continue
        if re.match(r'^\d+$', t):           # чисто цифра
            continue
        if re.match(r'^\d+[\.,]\d+$', t):   # дробное
            continue
        if re.match(r'^\d+[а-яa-z]{1,3}$', t, re.I) and len(t) <= 5:  # 4кг, 60л
            continue
        # Содержит и буквы и цифры — это модель
        if re.search(r'[A-Za-z]', t) and re.search(r'\d', t):
            candidates.append(t)
        # Аббревиатура из заглавных (IWSC, GTW, IRT)
        elif re.match(r'^[A-Z]{2,6}$', t):
            candidates.append(t)

    if not candidates:
        return None

    model = candidates[0]

    # Если аббревиатура без цифр — пробуем склеить со следующим числовым токеном
    if re.match(r'^[A-Z]{2,6}$', model):
        idx = next((i for i, t in enumerate(tokens)
                    if re.sub(r'[()[\]{}<>]', '', t).strip() == model), -1)
        if idx >= 0 and idx + 1 < len(tokens):
            nxt = re.sub(r'[()[\]{}<>]', '', tokens[idx + 1]).strip()
            if re.search(r'\d', nxt) and nxt.lower() not in SKIP_WORDS and len(nxt) >= 2:
                model = f"{model} {nxt}"

    return model if len(model) >= 3 else None


def normalize_model(model: str) -> str:
    return re.sub(r'[\s\-–_]', '', model).upper()


def resolve_subject_id(token: str, nm_id: int, subject_cache: dict,
                       subject_name: str, subject_id_cache: int | None = None) -> int | None:
    """
    Получает subject_id для ниши.
    1. Если wb_cards дал нам subject_id — используем его (0 MPSTATS-вызовов).
    2. Иначе кешируем: одна MPSTATS-вызов на уникальную нишу.
    """
    # Приоритет: subject_id из wb_cards
    if subject_id_cache:
        subject_cache[subject_name] = subject_id_cache
        return subject_id_cache

    if subject_name in subject_cache:
        return subject_cache[subject_name]

    # Один вызов MPSTATS /full для получения subject.id
    d = mpstats_get(token, f"items/{nm_id}/full")
    if d:
        subj = d.get("subject") or {}
        if isinstance(subj, dict):
            sid = subj.get("id") or (d.get("period_stats") or {}).get("subject_id")
            if sid:
                subject_cache[subject_name] = sid
                return sid

    subject_cache[subject_name] = None
    return None


def find_competitors(token: str, nm_id: int, subject_id: int, model: str,
                     limit: int = 300) -> list[dict]:
    """Ищет конкурентов всех брендов в той же нише по модельному номеру."""
    d1 = (date.today() - timedelta(days=32)).isoformat()
    d2 = (date.today() - timedelta(days=1)).isoformat()

    resp = mpstats_post(
        token,
        "subject/items",
        {"d1": d1, "d2": d2, "path": str(subject_id), "fbs": "1"},
        {
            "startRow": 0, "endRow": limit,
            "filterModel": {
                "name": {"filterType": "text", "type": "contains", "filter": model}
            },
            "sortModel": [{"colId": "revenue", "sort": "desc"}]
        }
    )

    items = resp.get("data", [])
    norm_model = normalize_model(model)
    result = []

    for p in items:
        comp_name = p.get("name", "")
        comp_nm = p.get("id")
        if not comp_nm or comp_nm == nm_id:
            continue
        # Убеждаемся что нормализованная модель входит в название конкурента
        if norm_model not in normalize_model(comp_name):
            continue
        result.append({
            "comp_nm_id":    comp_nm,
            "comp_name":     comp_name,
            "comp_brand":    p.get("brand", ""),
            "comp_seller":   p.get("seller", ""),
            "comp_seller_id": p.get("supplierId") or p.get("supplier_id"),
            "url":           f"https://www.wildberries.ru/catalog/{comp_nm}/detail.aspx",
        })

    return result


def save_competitors(conn: sqlite3.Connection, nm_id: int, our_name: str,
                     subject_id: int, subject_name: str,
                     competitors: list[dict]) -> int:
    today = date.today().isoformat()
    saved = 0
    for c in competitors:
        try:
            conn.execute("""
                INSERT INTO competitor_products
                    (cabinet_id, our_nm_id, our_name, comp_nm_id, comp_name,
                     comp_brand, comp_seller, comp_seller_id,
                     subject_id, subject_name, url, found_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(cabinet_id, our_nm_id, comp_nm_id) DO UPDATE SET
                    comp_name=excluded.comp_name, comp_brand=excluded.comp_brand,
                    comp_seller=excluded.comp_seller, comp_seller_id=excluded.comp_seller_id,
                    subject_name=excluded.subject_name, url=excluded.url,
                    found_at=excluded.found_at
            """, (
                CABINET_ID, nm_id, our_name,
                c["comp_nm_id"], c["comp_name"],
                c["comp_brand"], c["comp_seller"], c["comp_seller_id"],
                subject_id, subject_name, c["url"], today
            ))
            saved += 1
        except sqlite3.Error as e:
            print(f"  [WARN] save: {e}", file=sys.stderr)
    conn.commit()
    return saved


def main():
    parser = argparse.ArgumentParser(description="Поиск конкурентов для hld через MPSTATS")
    parser.add_argument("--test", action="store_true", help="Тест: только 2 артикула")
    args = parser.parse_args()

    token = load_mpstats_token()
    cab_db = os.path.join(BASE_DIR, f"../data/cabs/{CABINET_ID}.db")

    limit = 2 if args.test else None
    products = get_nm_ids_in_stock(cab_db, limit)

    if not products:
        print("[WARN] Нет товаров в наличии")
        return

    conn = sqlite3.connect(PRICES_DB)
    subject_cache: dict[str, int | None] = {}  # subject_name → subject_id
    total_found = total_saved = skipped_no_title = skipped_no_model = skipped_no_subj = 0

    for i, prod in enumerate(products, 1):
        nm_id        = prod["nm_id"]
        brand        = prod["brand"]
        subject_name = prod["subject_name"]
        title        = prod["title"]
        stock        = prod["stock"]

        print(f"\n[{i}/{len(products)}] nmId={nm_id} stock={stock}")
        print(f"  Бренд: {brand} | Ниша: {subject_name}")

        if not title:
            print(f"  [SKIP] Нет названия в raw_sales")
            skipped_no_title += 1
            time.sleep(0.3)
            continue

        print(f"  Название: {title}")

        model = extract_model(title, brand)
        if not model:
            print(f"  [SKIP] Не удалось извлечь модель из названия")
            skipped_no_model += 1
            time.sleep(0.3)
            continue

        print(f"  Модель: «{model}»")

        # Получаем subject_id (из wb_cards если есть, иначе один вызов MPSTATS на нишу)
        subject_id = resolve_subject_id(
            token, nm_id, subject_cache, subject_name,
            subject_id_cache=prod.get("subject_id_cache")
        )
        if not subject_id:
            print(f"  [SKIP] subject_id не определён для ниши «{subject_name}»")
            skipped_no_subj += 1
            time.sleep(0.5)
            continue

        # Ищем конкурентов
        competitors = find_competitors(token, nm_id, subject_id, model)
        total_found += len(competitors)
        print(f"  Конкурентов: {len(competitors)}")
        for c in competitors[:4]:
            print(f"    [{c['comp_nm_id']}] {c['comp_name'][:55]} | {c['comp_brand']}")

        saved = save_competitors(conn, nm_id, title, subject_id, subject_name, competitors)
        total_saved += saved

        time.sleep(1.2)

    conn.close()

    print(f"\n{'='*55}")
    print(f"Обработано:       {len(products)}")
    print(f"Нет названия:     {skipped_no_title}")
    print(f"Нет модели:       {skipped_no_model}")
    print(f"Нет subject_id:   {skipped_no_subj}")
    print(f"Найдено конкурентов: {total_found}")
    print(f"Сохранено:        {total_saved}")

    if args.test:
        print("\n✓ Тест готов. Для полной загрузки запустите без --test")


if __name__ == "__main__":
    main()
