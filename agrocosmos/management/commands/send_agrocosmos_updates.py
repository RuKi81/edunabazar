"""
Daily digest: notify subscribers when fresh NDVI data lands for their scope.

For each ``AgroSubscription`` with ``notify_updates=True``:
    - find ``VegetationIndex`` rows (``index_type='ndvi'``) whose
      ``created_at`` is newer than the subscription's
      ``last_update_notified_at`` (or, on first run, the last 24h)
      AND whose farmland lies in the subscription's scope;
    - if any exist, send one email and bump
      ``last_update_notified_at = now()``.

Safe to run daily.  Idempotent — re-running the same day won't
re-send anything because ``last_update_notified_at`` is advanced on
successful delivery.
"""
from __future__ import annotations

import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Count, Q
from django.utils import timezone

from agrocosmos.models import AgroSubscription, VegetationIndex
from agrocosmos.services.notifications import send_update_email

logger = logging.getLogger(__name__)

DEFAULT_FIRSTRUN_WINDOW_HOURS = 24


class Command(BaseCommand):
    help = 'Send daily NDVI update digest to AgroSubscription subscribers.'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true',
                            help='Print intended sends without emailing.')
        parser.add_argument('--firstrun-hours', type=int,
                            default=DEFAULT_FIRSTRUN_WINDOW_HOURS,
                            help='Hours of backlog to consider for subs that have '
                                 'never been notified before (default 24).')

    def handle(self, *args, **options):
        dry = options['dry_run']
        firstrun_hours = options['firstrun_hours']

        now = timezone.now()
        firstrun_since = now - timedelta(hours=firstrun_hours)

        subs = AgroSubscription.objects.filter(notify_updates=True).select_related(
            'region', 'district')

        sent = skipped = errors = 0
        for sub in subs:
            since = sub.last_update_notified_at or firstrun_since

            # Scope filter on VegetationIndex via farmland → district → region.
            q = VegetationIndex.objects.filter(
                index_type='ndvi',
                created_at__gt=since,
            )
            if sub.district_id:
                q = q.filter(farmland__district_id=sub.district_id)
            elif sub.region_id:
                q = q.filter(farmland__district__region_id=sub.region_id)
            else:
                continue  # constraint should prevent this; safety

            new_count = q.count()
            if new_count == 0:
                skipped += 1
                continue

            self.stdout.write(
                f'sub={sub.pk} user={sub.legacy_user_id} scope='
                f'{sub.district.name if sub.district_id else sub.region.name} '
                f'new={new_count}'
            )
            if dry:
                continue

            ok = False
            try:
                ok = send_update_email(sub, new_count)
            except Exception:
                logger.exception('Failed sending update email for sub=%s', sub.pk)

            if ok:
                sub.last_update_notified_at = now
                sub.save(update_fields=['last_update_notified_at', 'updated_at'])
                sent += 1
            else:
                errors += 1

        self.stdout.write(self.style.SUCCESS(
            f'Done: sent={sent}, skipped(no fresh data)={skipped}, errors={errors}'
            + (' [dry run]' if dry else '')
        ))
