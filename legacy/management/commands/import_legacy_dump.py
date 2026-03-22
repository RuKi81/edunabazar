"""
Import data from old MySQL/Strapi database dump (.gz) into current PostgreSQL/Django schema.

Usage:
    python manage.py import_legacy_dump /path/to/27-05.gz [--dry-run]

Mapping:
    up_users           → legacy_user
    categories (pid=null) → catalog, categories (pid!=null) → categories
    adverts + adverts_category_links + adverts_type_links → advert
    companies + companies_type_links → seller
"""

import gzip
import re
from datetime import datetime

from django.contrib.gis.geos import Point
from django.core.management.base import BaseCommand
from django.db import connection


# ---------------------------------------------------------------------------
#  MySQL INSERT parser (tested standalone)
# ---------------------------------------------------------------------------

def parse_mysql_dump(filepath: str, tables: list[str]) -> dict[str, list[tuple]]:
    """
    Read a .gz MySQL dump and extract rows for the requested tables.
    Returns {table_name: [tuple_of_values, ...]}.
    """
    result = {t: [] for t in tables}

    with gzip.open(filepath, 'rt', encoding='utf-8', errors='replace') as f:
        current_table = None
        accumulator = ""

        for raw_line in f:
            line = raw_line.rstrip('\n\r')

            # Try to match INSERT INTO `table` VALUES ...
            matched = False
            for t in tables:
                if line.strip() == f'INSERT INTO `{t}` VALUES':
                    # Flush previous accumulator
                    if current_table and accumulator.strip():
                        result[current_table].extend(_parse_values_line(accumulator))
                    current_table = t
                    accumulator = ""
                    matched = True
                    break
                prefix = f'INSERT INTO `{t}` VALUES '
                if line.startswith(prefix):
                    if current_table and accumulator.strip():
                        result[current_table].extend(_parse_values_line(accumulator))
                    current_table = t
                    accumulator = line[len(prefix):]
                    matched = True
                    break

            if matched:
                continue

            # Not an INSERT for our tables
            if current_table is not None:
                if (line.startswith('INSERT INTO') or line.startswith('DROP ')
                        or line.startswith('CREATE ') or line.startswith('ALTER ')
                        or line.startswith('LOCK ') or line.startswith('UNLOCK ')
                        or line.startswith('--') or line.startswith('/*!')):
                    if accumulator.strip():
                        result[current_table].extend(_parse_values_line(accumulator))
                    current_table = None
                    accumulator = ""
                else:
                    accumulator += line

        # Flush last accumulator
        if current_table and accumulator.strip():
            result[current_table].extend(_parse_values_line(accumulator))

    return result


def _parse_values_line(data: str) -> list[tuple]:
    """
    Parse MySQL VALUES data like: (1,'foo',NULL),(2,'bar',3);
    Returns list of tuples with Python values.
    """
    rows = []
    data = data.strip().rstrip(';').rstrip(',')
    if not data:
        return rows

    i = 0
    while i < len(data):
        # Skip whitespace and commas between rows
        while i < len(data) and data[i] in (' ', '\t', '\n', '\r', ','):
            i += 1
        if i >= len(data):
            break
        if data[i] != '(':
            i += 1
            continue

        # Parse a single tuple (...)
        i += 1  # skip '('
        values = []
        while i < len(data):
            # Skip whitespace
            while i < len(data) and data[i] in (' ', '\t'):
                i += 1
            if i >= len(data):
                break
            if data[i] == ')':
                i += 1
                break

            # Parse a value
            if data[i] == '\'':
                # String value
                val, i = _parse_string(data, i)
                values.append(val)
            elif data[i:i+4].upper() == 'NULL':
                values.append(None)
                i += 4
            else:
                # Numeric value
                j = i
                while j < len(data) and data[j] not in (',', ')'):
                    j += 1
                val_str = data[i:j].strip()
                if '.' in val_str:
                    try:
                        values.append(float(val_str))
                    except ValueError:
                        values.append(val_str)
                else:
                    try:
                        values.append(int(val_str))
                    except ValueError:
                        values.append(val_str)
                i = j

            # Skip comma between values
            if i < len(data) and data[i] == ',':
                i += 1

        rows.append(tuple(values))

    return rows


def _parse_string(data: str, pos: int) -> tuple:
    """Parse a MySQL single-quoted string starting at pos. Returns (value, new_pos)."""
    assert data[pos] == '\''
    pos += 1
    chars = []
    while pos < len(data):
        c = data[pos]
        if c == '\\' and pos + 1 < len(data):
            nc = data[pos + 1]
            if nc == '\'':
                chars.append('\'')
            elif nc == '\\':
                chars.append('\\')
            elif nc == 'n':
                chars.append('\n')
            elif nc == 'r':
                chars.append('\r')
            elif nc == 't':
                chars.append('\t')
            elif nc == '0':
                chars.append('\0')
            else:
                chars.append(nc)
            pos += 2
        elif c == '\'' and pos + 1 < len(data) and data[pos + 1] == '\'':
            # Doubled single quote
            chars.append('\'')
            pos += 2
        elif c == '\'':
            pos += 1
            break
        else:
            chars.append(c)
            pos += 1
    return ''.join(chars), pos


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def parse_dt(val) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    if not s:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_coords(coords_str) -> Point | None:
    """Parse 'lon,lat' or 'lat,lon' string into a PostGIS Point(lon, lat)."""
    if not coords_str:
        return None
    try:
        parts = str(coords_str).split(',')
        if len(parts) != 2:
            return None
        a, b = float(parts[0].strip()), float(parts[1].strip())
        # Strapi stored as "lon,lat" based on the sample: '39.610422,45.54491'
        # lon ~39 (longitude for Russia), lat ~45 (latitude for Russia) — this looks correct
        # Actually for Krasnodar region: lon ~39, lat ~45 — yes, (lon, lat)
        lon, lat = a, b
        if abs(lon) > 180 or abs(lat) > 90:
            # Maybe swapped
            lon, lat = lat, lon
        if abs(lon) > 180 or abs(lat) > 90:
            return None
        return Point(lon, lat, srid=4326)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
#  Command
# ---------------------------------------------------------------------------

class Command(BaseCommand):
    help = 'Import data from old MySQL/Strapi dump into current Django PostgreSQL DB'

    def add_arguments(self, parser):
        parser.add_argument('dump_file', help='Path to the .gz MySQL dump file')
        parser.add_argument('--dry-run', action='store_true', help='Parse and report counts without inserting')

    def handle(self, *args, **options):
        dump_file = options['dump_file']
        dry_run = options['dry_run']

        self.stdout.write(f'Parsing dump: {dump_file}')

        tables_needed = [
            'up_users',
            'categories',
            'adverts',
            'adverts_category_links',
            'adverts_type_links',
            'companies',
            'companies_type_links',
            'advert_types',
            'advert_unit_types',
            'currency_types',
            'orginzation_types',
        ]
        data = parse_mysql_dump(dump_file, tables_needed)

        for t in tables_needed:
            self.stdout.write(f'  {t}: {len(data[t])} rows')

        if dry_run:
            self.stdout.write(self.style.SUCCESS('Dry run — no data inserted.'))
            return

        from django.db import transaction
        try:
            with transaction.atomic():
                with connection.cursor() as cur:
                    self._import_users(cur, data['up_users'])
                    cat_map, catalog_map = self._import_categories(cur, data['categories'])
                    self._import_adverts(cur, data['adverts'], data['adverts_category_links'],
                                         data['adverts_type_links'], cat_map)
                    self._import_sellers(cur, data['companies'], data['companies_type_links'])
            self.stdout.write(self.style.SUCCESS('Import complete!'))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f'Import FAILED — transaction rolled back: {e}'))
            raise

    # ------------------------------------------------------------------
    #  Users: up_users → legacy_user
    # ------------------------------------------------------------------
    def _import_users(self, cur, rows):
        """
        up_users columns (from CREATE TABLE):
        id, username, email, provider, password, reset_password_token,
        confirmation_token, confirmed, blocked, created_at, updated_at,
        created_by_id, updated_by_id, uid
        """
        self.stdout.write(f'Importing {len(rows)} users...')
        count = 0
        for row in rows:
            try:
                uid = row[0]
                username = row[1] or f'user_{uid}'
                email = row[2] or ''
                password_hash = row[4] or ''
                confirmed = row[7]
                blocked = row[8]
                created_at = parse_dt(row[9])
                updated_at = parse_dt(row[10])
                strapi_uid = row[13] if len(row) > 13 else None

                # Map status: confirmed=1 & not blocked → 10 (active)
                if blocked:
                    status = 0
                elif confirmed:
                    status = 10
                else:
                    status = 5

                cur.execute("""
                    INSERT INTO legacy_user
                        (id, type, username, auth_key, password_hash, email,
                         currency, name, address, phone, inn, status,
                         created_at, updated_at, contacts)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, [
                    uid, 0, username[:255], '', password_hash[:255], email[:255],
                    'RUB', username[:255], '', '', '', status,
                    created_at or datetime.now(), updated_at or datetime.now(), '',
                ])
                count += 1
            except Exception as e:
                self.stderr.write(f'  User #{row[0]}: {e}')
        self.stdout.write(f'  Users inserted: {count}')
        # Reset sequence
        cur.execute("SELECT setval('legacy_user_id_seq', COALESCE((SELECT MAX(id) FROM legacy_user), 1))")

    # ------------------------------------------------------------------
    #  Categories: categories → catalog + categories
    # ------------------------------------------------------------------
    def _import_categories(self, cur, rows):
        """
        categories columns:
        id, name, pid, active, created_at, updated_at, published_at,
        created_by_id, updated_by_id
        """
        self.stdout.write(f'Importing {len(rows)} categories...')

        # Build tree: pid=NULL → top-level (catalog), pid!=NULL → subcategory
        top_level = []
        children = {}
        for row in rows:
            cat_id = row[0]
            name = row[1] or f'Category {cat_id}'
            pid = row[2]  # parent id
            active = 1 if row[3] else 0

            if pid is None:
                top_level.append((cat_id, name, active))
            else:
                children.setdefault(pid, []).append((cat_id, name, active))

        self.stdout.write(f'  Top-level (catalogs): {len(top_level)}, subcategories: {sum(len(v) for v in children.values())}')

        # Insert catalogs
        catalog_map = {}  # old_cat_id → new_catalog_id
        sort_idx = 0
        for old_id, name, active in top_level:
            sort_idx += 1
            cur.execute("""
                INSERT INTO catalog (title, sort, active)
                VALUES (%s, %s, %s)
                RETURNING id
            """, [name[:255], sort_idx, active])
            new_id = cur.fetchone()[0]
            catalog_map[old_id] = new_id

        self.stdout.write(f'  Catalogs inserted: {len(catalog_map)}')

        # Build a mapping: for any old_cat_id, find the root catalog
        # This handles 3+ levels of nesting by walking up the tree
        all_cats = {}  # old_id → (name, pid, active)
        for row in rows:
            all_cats[row[0]] = (row[1] or f'Category {row[0]}', row[2], 1 if row[3] else 0)

        def find_root(cat_id, visited=None):
            """Walk up the tree to find the top-level (catalog) ancestor."""
            if visited is None:
                visited = set()
            if cat_id in visited:
                return None
            visited.add(cat_id)
            if cat_id in catalog_map:
                return cat_id
            info = all_cats.get(cat_id)
            if info is None:
                return None
            parent = info[1]
            if parent is None:
                return cat_id  # is top-level
            return find_root(parent, visited)

        # Insert subcategories — assign each to its root catalog
        cat_map = {}  # old_cat_id → new_categories_id
        cat_count = 0
        orphan_count = 0
        for old_id, (name, pid, active) in all_cats.items():
            if pid is None:
                continue  # skip top-level (already inserted as catalog)
            root_id = find_root(pid)
            catalog_id = catalog_map.get(root_id) if root_id else None
            if catalog_id is None:
                orphan_count += 1
                continue
            try:
                cur.execute("""
                    INSERT INTO categories (catalog, title, active)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, [catalog_id, name[:255], active])
                new_id = cur.fetchone()[0]
                cat_map[old_id] = new_id
                cat_count += 1
            except Exception as e:
                self.stderr.write(f'  Category "{name}": {e}')

        if orphan_count:
            self.stdout.write(f'  WARNING: {orphan_count} orphan categories skipped (no root catalog found)')

        self.stdout.write(f'  Categories inserted: {cat_count}')
        # Reset sequences
        cur.execute("SELECT setval('catalog_id_seq', COALESCE((SELECT MAX(id) FROM catalog), 1))")
        cur.execute("SELECT setval('categories_id_seq', COALESCE((SELECT MAX(id) FROM categories), 1))")

        return cat_map, catalog_map

    # ------------------------------------------------------------------
    #  Adverts
    # ------------------------------------------------------------------
    def _import_adverts(self, cur, rows, cat_links, type_links, cat_map):
        """
        adverts columns:
        id, name, price, batch_price, userid, address,
        created_at, updated_at, published_at, created_by_id, updated_by_id,
        desc, capacity_min, capacity_max, batch_min_number, active,
        uid, coords, views, razmernost_tovara

        adverts_category_links: (advert_id, category_id)
        adverts_type_links: (advert_id, type_id)
        """
        self.stdout.write(f'Importing {len(rows)} adverts...')

        # Build lookup maps
        advert_cat = {}
        for link in cat_links:
            advert_cat[link[0]] = link[1]

        advert_type = {}
        for link in type_links:
            advert_type[link[0]] = link[1]

        # Get new category IDs for validation
        cur.execute("SELECT id FROM categories")
        new_cat_ids = {r[0] for r in cur.fetchall()}

        # Also check existing users
        cur.execute("SELECT id FROM legacy_user")
        existing_users = {r[0] for r in cur.fetchall()}

        # Fallback category
        fallback_cat = min(new_cat_ids) if new_cat_ids else None

        count = 0
        skipped = 0
        cat_miss = 0
        for row in rows:
            try:
                advert_id = row[0]
                name = row[1] or 'Без названия'
                price = row[2] or 0
                batch_price = row[3] or 0
                userid = row[4]
                address = row[5] or ''
                created_at = parse_dt(row[6])
                updated_at = parse_dt(row[7])
                published_at = parse_dt(row[8])
                desc_text = row[11] or ''
                capacity_min = row[12] or 0
                capacity_max = row[13] or 0
                batch_min = row[14] or 0
                active = row[15]
                coords_str = row[17] if len(row) > 17 else None

                # Map old category id → new category id
                old_cat_id = advert_cat.get(advert_id)
                category_id = cat_map.get(old_cat_id)
                if category_id is None:
                    category_id = fallback_cat
                    cat_miss += 1
                if category_id is None:
                    skipped += 1
                    continue

                # Check user exists
                if userid not in existing_users:
                    skipped += 1
                    continue

                # Map type: 1=предложение(0), 2=спрос(1)
                adv_type = advert_type.get(advert_id, 1)
                mapped_type = 0 if adv_type == 1 else 1

                # Status: active=1 → 10, else 5
                status = 10 if active else 5

                # Location
                location = parse_coords(coords_str)
                if location is None:
                    location = Point(37.6173, 55.7558, srid=4326)  # Moscow default

                # Clean HTML from description
                desc_clean = re.sub(r'<br\s*/?>', '\n', desc_text)
                desc_clean = re.sub(r'<[^>]+>', '', desc_clean)

                cur.execute("""
                    INSERT INTO advert
                        (id, type, category, author, address, location, delivery,
                         contacts, title, text, price, wholesale_price,
                         min_volume, wholesale_volume, volume,
                         priority, created_at, updated_at, price_unit, status)
                    VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s,
                            %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, [
                    advert_id, mapped_type, category_id, userid,
                    address[:255], location.wkt if location else None, False,
                    '', name[:255], desc_clean, price, batch_price,
                    batch_min, capacity_min, capacity_max,
                    0, created_at or datetime.now(), updated_at or datetime.now(),
                    'кг', status,
                ])
                count += 1
            except Exception as e:
                self.stderr.write(f'  Advert #{row[0]}: {e}')

        self.stdout.write(f'  Adverts inserted: {count}, skipped: {skipped}, category fallback: {cat_miss}')
        cur.execute("SELECT setval('advert_id_seq', COALESCE((SELECT MAX(id) FROM advert), 1))")

    # ------------------------------------------------------------------
    #  Sellers (companies → seller)
    # ------------------------------------------------------------------
    def _import_sellers(self, cur, rows, type_links):
        """
        companies columns:
        id, shortname, fullname, description, address, userid, products,
        inn, coords, active, created_at, updated_at, published_at,
        created_by_id, updated_by_id, uid, views
        """
        self.stdout.write(f'Importing {len(rows)} sellers...')

        # Check existing users
        cur.execute("SELECT id FROM legacy_user")
        existing_users = {r[0] for r in cur.fetchall()}

        count = 0
        skipped = 0
        for row in rows:
            try:
                company_id = row[0]
                shortname = row[1] or ''
                fullname = row[2] or ''
                description = row[3] or ''
                address = row[4] or ''
                userid = row[5]
                products = row[6] or ''
                inn = row[7] or ''
                active = row[9]
                created_at = parse_dt(row[10])
                updated_at = parse_dt(row[11])

                name = shortname or fullname or f'Компания #{company_id}'

                # Status: active=1 → 10, else 5
                status = 10 if active else 5

                # User check — sellers need a valid user
                if userid and userid not in existing_users:
                    # Create a placeholder user
                    cur.execute("""
                        INSERT INTO legacy_user
                            (id, type, username, auth_key, password_hash, email,
                             currency, name, address, phone, inn, status,
                             created_at, updated_at, contacts)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING
                    """, [
                        userid, 1, f'company_user_{userid}', '', '', '',
                        'RUB', name[:255], '', '', inn[:20], 10,
                        created_at or datetime.now(), updated_at or datetime.now(), '',
                    ])
                    existing_users.add(userid)

                if not userid:
                    skipped += 1
                    continue

                # Clean HTML
                desc_clean = re.sub(r'<br\s*/?>', '\n', description)
                desc_clean = re.sub(r'<[^>]+>', '', desc_clean)
                products_clean = re.sub(r'<br\s*/?>', '\n', products)
                products_clean = re.sub(r'<[^>]+>', '', products_clean)

                cur.execute("""
                    INSERT INTO seller
                        (id, "user", name, logo, location, contacts,
                         price_list, links, about,
                         created_at, updated_at, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, [
                    company_id, userid, name[:255], 0, address,
                    '{}', 0, products_clean, desc_clean,
                    created_at or datetime.now(), updated_at or datetime.now(), status,
                ])
                count += 1
            except Exception as e:
                self.stderr.write(f'  Seller #{row[0]}: {e}')

        self.stdout.write(f'  Sellers inserted: {count}, skipped: {skipped}')
        cur.execute("SELECT setval('seller_id_seq', COALESCE((SELECT MAX(id) FROM seller), 1))")
