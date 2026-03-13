import argparse
import os
import random
import sys
import time
import uuid
import urllib.parse
import urllib.request
import json
from datetime import datetime, timezone

try:
    import psycopg
except Exception:
    psycopg = None

try:
    import psycopg2
except Exception:
    psycopg2 = None


UNSPLASH_SOURCE_URL = "https://source.unsplash.com/1600x1000/?{query}"
WIKIMEDIA_API_URL = "https://commons.wikimedia.org/w/api.php"


def _get_db_conn():
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", "5432"))
    user = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "admin")
    db = os.getenv("DB_NAME", "enb_DB")

    if psycopg is not None:
        return psycopg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=db,
            autocommit=True,
            options="-c client_encoding=UTF8",
        )
    if psycopg2 is not None:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=db,
            options="-c client_encoding=UTF8",
        )
        conn.autocommit = True
        return conn

    raise RuntimeError("PostgreSQL driver is not installed (psycopg or psycopg2)")


def _has_column(conn, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name=%s AND column_name=%s
            LIMIT 1
            """,
            (table, column),
        )
        return cur.fetchone() is not None


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _download_unsplash_image(query: str, dst_path: str, timeout: int = 30) -> None:
    url = UNSPLASH_SOURCE_URL.format(query=urllib.parse.quote(query))
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    with open(dst_path, "wb") as f:
        f.write(data)


def _download_wikimedia_image(query: str, dst_path: str, timeout: int = 30) -> tuple[bool, str]:
    try:
        params = {
            "action": "query",
            "format": "json",
            "origin": "*",
            "generator": "search",
            "gsrsearch": f"{query} filetype:bitmap",
            "gsrnamespace": "6",
            "gsrlimit": "10",
            "prop": "imageinfo",
            "iiprop": "url",
        }
        url = WIKIMEDIA_API_URL + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read().decode("utf-8", errors="replace")
        data = json.loads(payload)

        pages = (data.get("query") or {}).get("pages") or {}
        urls = []
        for p in pages.values():
            infos = p.get("imageinfo") or []
            if infos and isinstance(infos, list):
                u = (infos[0] or {}).get("url")
                if u:
                    urls.append(str(u))
        if not urls:
            return False, "no results"

        random.shuffle(urls)
        img_url = urls[0]
        req2 = urllib.request.Request(img_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req2, timeout=timeout) as resp2:
            img = resp2.read()
        with open(dst_path, "wb") as f:
            f.write(img)

        if os.path.exists(dst_path) and os.path.getsize(dst_path) > 5_000:
            return True, ""
        return False, "downloaded file too small"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _download_unsplash_image_with_retries(query: str, dst_path: str, retries: int = 4, timeout: int = 30) -> tuple[bool, str]:
    last_err = ""
    for attempt in range(retries):
        try:
            _download_unsplash_image(query, dst_path, timeout=timeout)
            if os.path.exists(dst_path) and os.path.getsize(dst_path) > 5_000:
                return True, ""
            last_err = "downloaded file too small"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
        time.sleep(0.75 * (attempt + 1))
    return False, last_err


def _download_image(query: str, dst_path: str) -> tuple[bool, str, str]:
    ok, err = _download_unsplash_image_with_retries(query, dst_path)
    if ok:
        return True, "", "unsplash"

    ok2, err2 = _download_wikimedia_image(query, dst_path)
    if ok2:
        return True, "", "wikimedia"
    return False, f"unsplash: {err}; wikimedia: {err2}", ""


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Seed demo adverts with classifier + photos")
    p.add_argument("--count", type=int, default=50)
    p.add_argument("--photos-per-advert", type=int, default=1)
    p.add_argument("--no-photos", action="store_true")
    p.add_argument(
        "--attach-missing-photos",
        action="store_true",
        help="Do not create adverts; attach photos to existing adverts that have no advert_photo rows",
    )
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--status", type=int, default=10)
    args = p.parse_args(argv)

    random.seed(args.seed)

    conn = _get_db_conn()
    try:
        has_delivery = _has_column(conn, "advert", "delivery")
        has_address = _has_column(conn, "advert", "address")
        has_price_unit = _has_column(conn, "advert", "price_unit")

        with conn.cursor() as cur:
            cur.execute("SELECT id FROM legacy_user ORDER BY id ASC LIMIT 1")
            row = cur.fetchone()
            if not row:
                raise RuntimeError("No legacy_user found: create at least one user first")
            author_id = int(row[0])

            cur.execute("SELECT id, title, catalog FROM categories WHERE active=1 ORDER BY id ASC")
            categories = [(int(r[0]), str(r[1] or ""), int(r[2])) for r in (cur.fetchall() or [])]
            if not categories:
                raise RuntimeError("No active categories found")

            cur.execute("SELECT id, title FROM catalog WHERE active=1 ORDER BY id ASC")
            catalogs = {int(r[0]): str(r[1] or "") for r in (cur.fetchall() or [])}

        queries_by_catalog = {
            1: ["wheat", "grain", "barley"],
            2: ["vegetables", "tomato", "potato"],
            3: ["fruit", "apple", "berries"],
            4: ["milk", "cheese", "dairy"],
            5: ["meat", "beef", "chicken"],
            6: ["fish", "salmon", "seafood"],
            7: ["honey", "bee", "hive"],
            8: ["fertilizer", "tractor", "farm"],
            9: ["seedlings", "greenhouse", "plants"],
            10: ["hay", "feed", "corn"],
        }

        def catalog_query(catalog_id: int) -> str:
            qlist = queries_by_catalog.get(catalog_id) or ["farm", "agriculture"]
            return random.choice(qlist)

        media_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "media")
        rel_dir = os.path.join("adverts", "photos")
        abs_dir = os.path.join(media_root, rel_dir)
        _ensure_dir(abs_dir)

        now = datetime.now(timezone.utc)

        created_ids: list[int] = []
        attached_ids: list[int] = []

        download_ok = 0
        download_fail = 0
        download_fail_samples: list[str] = []
        ok_by_source = {"unsplash": 0, "wikimedia": 0}

        if args.attach_missing_photos:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT a.id, c.catalog
                    FROM advert a
                    JOIN categories c ON c.id=a.category
                    LEFT JOIN advert_photo ap ON ap.advert_id=a.id
                    WHERE ap.id IS NULL
                    ORDER BY a.id ASC
                    LIMIT %s
                    """,
                    (int(args.count),),
                )
                targets = [(int(r[0]), int(r[1])) for r in (cur.fetchall() or [])]

            for advert_id, catalog_id in targets:
                if args.no_photos or args.photos_per_advert <= 0:
                    continue

                inserted_any = False
                for s in range(args.photos_per_advert):
                    q = catalog_query(catalog_id)
                    fname = f"{uuid.uuid4().hex}.jpg"
                    abs_path = os.path.join(abs_dir, fname)
                    rel_path = "/".join(["adverts", "photos", fname])

                    ok, err, src = _download_image(q, abs_path)
                    if not ok:
                        download_fail += 1
                        if len(download_fail_samples) < 8:
                            download_fail_samples.append(f"{q}: {err}")
                        try:
                            if os.path.exists(abs_path):
                                os.remove(abs_path)
                        except Exception:
                            pass
                        continue

                    download_ok += 1
                    if src:
                        ok_by_source[src] = ok_by_source.get(src, 0) + 1
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO advert_photo (advert_id, image, sort, created_at)
                            VALUES (%s, %s, %s, NOW())
                            """,
                            (advert_id, rel_path, s),
                        )

                    inserted_any = True

                if inserted_any:
                    attached_ids.append(advert_id)

            print(f"Attached photos to adverts: {len(attached_ids)}")
            print(f"Downloads ok: {download_ok}; failed: {download_fail}")
            if download_ok:
                print(f"Downloads by source: {ok_by_source}")
            if download_fail_samples:
                print("Sample download errors:")
                for s in download_fail_samples:
                    print("- " + s)
            return 0

        # Mix combinations to ensure checkboxes coverage
        combos = []
        for advert_type in (0, 1):
            for opt in (0, 1):
                for delivery in (0, 1):
                    combos.append((advert_type, opt, delivery))
        random.shuffle(combos)

        for i in range(args.count):
            cat_id, cat_title, catalog_id = categories[i % len(categories)]
            catalog_title = catalogs.get(catalog_id, "")

            advert_type, opt, delivery = combos[i % len(combos)]

            base_price = float(random.randint(50, 5000))
            wholesale_price = base_price if opt else 0.0

            price_unit = random.choice(["кг", "л", "т", "шт"]) if has_price_unit else None

            title = (
                f"{catalog_title}: {cat_title} — {'продам' if advert_type == 0 else 'куплю'}"
            ).strip()[:250]

            text = (
                f"Демо-объявление #{i + 1}. "
                f"Каталог: {catalog_title}. Категория: {cat_title}. "
                f"Тип: {'предложение' if advert_type == 0 else 'спрос'}. "
                f"Опт: {'да' if opt else 'нет'}. Доставка: {'да' if delivery else 'нет'}."
            )

            # Roughly around Moscow
            lat = 55.55 + random.random() * 0.6
            lon = 37.25 + random.random() * 0.9

            cols = [
                "type",
                "category",
                "author",
                "contacts",
                "title",
                "text",
                "price",
                "wholesale_price",
                "min_volume",
                "wholesale_volume",
                "volume",
                "priority",
                "created_at",
                "updated_at",
                "status",
                "location",
            ]
            vals = [
                "%s",
                "%s",
                "%s",
                "%s",
                "%s",
                "%s",
                "%s",
                "%s",
                "%s",
                "%s",
                "%s",
                "0",
                "%s",
                "%s",
                "%s",
                "ST_SetSRID(ST_MakePoint(%s, %s), 4326)",
            ]
            params = [
                advert_type,
                cat_id,
                author_id,
                "+79990000000",
                title,
                text,
                base_price,
                wholesale_price,
                float(random.choice([0, 1, 5, 10])),
                float(random.choice([0, 10, 20, 50])),
                float(random.choice([0, 1, 10, 100])),
                now,
                now,
                int(args.status),
                float(lon),
                float(lat),
            ]

            if has_delivery:
                cols.insert(4, "delivery")
                vals.insert(4, "%s")
                params.insert(4, bool(delivery))

            if has_address:
                cols.insert(4, "address")
                vals.insert(4, "%s")
                params.insert(4, f"Москва, демо адрес {i + 1}")

            if has_price_unit:
                cols.insert(4, "price_unit")
                vals.insert(4, "%s")
                params.insert(4, price_unit or "")

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO advert
                        ({', '.join(cols)})
                    VALUES
                        ({', '.join(vals)})
                    RETURNING id
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
                advert_id = int(row[0])
                created_ids.append(advert_id)

            if not args.no_photos and args.photos_per_advert > 0:
                for s in range(args.photos_per_advert):
                    q = catalog_query(catalog_id)
                    fname = f"{uuid.uuid4().hex}.jpg"
                    abs_path = os.path.join(abs_dir, fname)
                    rel_path = "/".join(["adverts", "photos", fname])

                    ok, err, src = _download_image(q, abs_path)
                    if not ok:
                        download_fail += 1
                        if len(download_fail_samples) < 8:
                            download_fail_samples.append(f"{q}: {err}")
                        try:
                            if os.path.exists(abs_path):
                                os.remove(abs_path)
                        except Exception:
                            pass
                        continue

                    download_ok += 1
                    if src:
                        ok_by_source[src] = ok_by_source.get(src, 0) + 1

                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO advert_photo (advert_id, image, sort, created_at)
                            VALUES (%s, %s, %s, NOW())
                            """,
                            (advert_id, rel_path, s),
                        )

        print(f"Created adverts: {len(created_ids)}")
        if created_ids:
            print(f"First id: {created_ids[0]}")
            print(f"Last id: {created_ids[-1]}")

        if not args.no_photos and args.photos_per_advert > 0:
            print(f"Downloads ok: {download_ok}; failed: {download_fail}")
            if download_ok:
                print(f"Downloads by source: {ok_by_source}")
            if download_fail_samples:
                print("Sample download errors:")
                for s in download_fail_samples:
                    print("- " + s)

        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
