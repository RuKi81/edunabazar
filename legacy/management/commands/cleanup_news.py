"""
Management command to review and deactivate irrelevant news articles using GigaChat.

Usage:
    python manage.py cleanup_news          # check & deactivate irrelevant
    python manage.py cleanup_news --dry    # preview without changes
    python manage.py cleanup_news --limit=50
"""

from django.core.management.base import BaseCommand

from legacy.models import News


class Command(BaseCommand):
    help = 'Deactivate irrelevant news articles using GigaChat relevance check'

    def add_arguments(self, parser):
        parser.add_argument('--dry', action='store_true', help='Preview without deactivating')
        parser.add_argument('--limit', type=int, default=0, help='Max articles to check (0 = all)')

    def handle(self, *args, **options):
        from legacy.management.commands.fetch_news import _check_relevance_gigachat

        dry = options['dry']
        limit = options['limit']

        qs = News.objects.filter(is_active=True).order_by('-published_at')
        if limit > 0:
            qs = qs[:limit]

        total = qs.count()
        self.stdout.write(f'Checking {total} active news articles...')

        deactivated = 0
        kept = 0
        errors = 0

        for news in qs:
            try:
                is_relevant = _check_relevance_gigachat(news.title, news.text)
            except Exception as e:
                self.stdout.write(self.style.WARNING(f'  Error checking #{news.id}: {e}'))
                errors += 1
                continue

            if is_relevant:
                kept += 1
                continue

            self.stdout.write(self.style.WARNING(
                f'  IRRELEVANT: [{news.published_at}] {news.title[:80]}'
            ))
            if not dry:
                news.is_active = False
                news.save(update_fields=['is_active'])
            deactivated += 1

        action = 'Would deactivate' if dry else 'Deactivated'
        self.stdout.write(self.style.SUCCESS(
            f'\nDone. {action}: {deactivated}, Kept: {kept}, Errors: {errors}'
        ))
