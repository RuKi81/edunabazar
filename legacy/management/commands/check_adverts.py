"""
Diagnostic command: check advert↔user linkage in the database.

Usage:
    python manage.py check_adverts
    python manage.py check_adverts --user 3556
"""

from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = 'Check advert↔user linkage and report issues'

    def add_arguments(self, parser):
        parser.add_argument('--user', type=int, help='Check a specific user ID')

    def handle(self, *args, **options):
        user_id = options.get('user')

        with connection.cursor() as cur:
            # Overall stats
            cur.execute("SELECT COUNT(*) FROM legacy_user")
            total_users = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM advert")
            total_adverts = cur.fetchone()[0]
            self.stdout.write(f'Total users: {total_users}')
            self.stdout.write(f'Total adverts: {total_adverts}')

            # Users with adverts
            cur.execute("""
                SELECT COUNT(DISTINCT u.id)
                FROM legacy_user u
                JOIN advert a ON a.author = u.id
            """)
            users_with_adverts = cur.fetchone()[0]
            self.stdout.write(f'Users WITH adverts: {users_with_adverts}')
            self.stdout.write(f'Users WITHOUT adverts: {total_users - users_with_adverts}')

            # Orphaned adverts (author not in legacy_user)
            cur.execute("""
                SELECT COUNT(*)
                FROM advert a
                LEFT JOIN legacy_user u ON u.id = a.author
                WHERE u.id IS NULL
            """)
            orphan_adverts = cur.fetchone()[0]
            if orphan_adverts:
                self.stdout.write(self.style.WARNING(
                    f'Orphaned adverts (author not in legacy_user): {orphan_adverts}'
                ))
                cur.execute("""
                    SELECT a.author, COUNT(*) as cnt
                    FROM advert a
                    LEFT JOIN legacy_user u ON u.id = a.author
                    WHERE u.id IS NULL
                    GROUP BY a.author
                    ORDER BY cnt DESC
                    LIMIT 20
                """)
                for row in cur.fetchall():
                    self.stdout.write(f'  author={row[0]} → {row[1]} adverts (user missing)')
            else:
                self.stdout.write(self.style.SUCCESS('No orphaned adverts.'))

            # Top 10 users by advert count
            self.stdout.write('\nTop 10 users by advert count:')
            cur.execute("""
                SELECT u.id, u.username, COUNT(a.id) as cnt
                FROM legacy_user u
                LEFT JOIN advert a ON a.author = u.id
                GROUP BY u.id, u.username
                ORDER BY cnt DESC
                LIMIT 10
            """)
            for row in cur.fetchall():
                self.stdout.write(f'  user {row[0]} ({row[1]}): {row[2]} adverts')

            # Specific user check
            if user_id:
                self.stdout.write(f'\n--- Checking user {user_id} ---')
                cur.execute("SELECT id, username, email, name FROM legacy_user WHERE id = %s", [user_id])
                u = cur.fetchone()
                if u:
                    self.stdout.write(f'  Found: id={u[0]}, username={u[1]}, email={u[2]}, name={u[3]}')
                else:
                    self.stdout.write(self.style.ERROR(f'  User {user_id} NOT FOUND in legacy_user'))

                cur.execute("SELECT COUNT(*) FROM advert WHERE author = %s", [user_id])
                cnt = cur.fetchone()[0]
                self.stdout.write(f'  Adverts with author={user_id}: {cnt}')

                if cnt == 0:
                    # Check if there are adverts with nearby IDs
                    cur.execute("""
                        SELECT DISTINCT author FROM advert
                        WHERE author BETWEEN %s AND %s
                        ORDER BY author
                    """, [user_id - 10, user_id + 10])
                    nearby = [r[0] for r in cur.fetchall()]
                    if nearby:
                        self.stdout.write(f'  Nearby author IDs in advert table: {nearby}')
                    else:
                        self.stdout.write('  No adverts with nearby author IDs either.')

                cur.execute("""
                    SELECT id, title, status, created_at
                    FROM advert WHERE author = %s
                    ORDER BY created_at DESC LIMIT 5
                """, [user_id])
                rows = cur.fetchall()
                if rows:
                    self.stdout.write(f'  Latest adverts:')
                    for r in rows:
                        self.stdout.write(f'    id={r[0]}, title={r[1]!r}, status={r[2]}, created={r[3]}')
