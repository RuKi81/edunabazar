"""
Backfill empty advert.contacts from the author's phone / email / username.

Usage:
    python manage.py backfill_advert_contacts [--dry-run]
"""

from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = 'Fill empty advert.contacts from the author (phone → email → username)'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Show counts without updating')

    def handle(self, *args, **options):
        dry_run = options['dry_run']

        with connection.cursor() as cur:
            # Count adverts with empty contacts
            cur.execute("""
                SELECT COUNT(*) FROM advert
                WHERE contacts IS NULL OR TRIM(contacts) = ''
            """)
            total = cur.fetchone()[0]
            self.stdout.write(f'Adverts with empty contacts: {total}')

            if dry_run:
                self.stdout.write(self.style.WARNING('Dry run — no changes made.'))
                return

            # Update contacts from author: phone → email → username
            cur.execute("""
                UPDATE advert a
                SET contacts = COALESCE(
                    NULLIF(TRIM(u.phone), ''),
                    NULLIF(TRIM(u.email), ''),
                    NULLIF(TRIM(u.username), ''),
                    ''
                )
                FROM legacy_user u
                WHERE a.author = u.id
                  AND (a.contacts IS NULL OR TRIM(a.contacts) = '')
            """)
            updated = cur.rowcount
            self.stdout.write(self.style.SUCCESS(f'Updated {updated} adverts'))
