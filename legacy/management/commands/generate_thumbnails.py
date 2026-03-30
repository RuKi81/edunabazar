"""
Management command to generate WebP thumbnails for existing AdvertPhoto records.

Usage:
    python manage.py generate_thumbnails          # all photos without thumbnails
    python manage.py generate_thumbnails --all     # regenerate all thumbnails
    python manage.py generate_thumbnails --limit=100
"""

import os

from django.conf import settings
from django.core.management.base import BaseCommand

from legacy.models import AdvertPhoto
from legacy.image_utils import generate_thumbnail_for_existing


class Command(BaseCommand):
    help = 'Generate WebP thumbnails for existing advert photos'

    def add_arguments(self, parser):
        parser.add_argument('--all', action='store_true',
                            help='Regenerate thumbnails for all photos (not just missing)')
        parser.add_argument('--limit', type=int, default=0,
                            help='Max photos to process (0 = unlimited)')

    def handle(self, *args, **options):
        regen_all = options['all']
        limit = options['limit']

        qs = AdvertPhoto.objects.all().order_by('id')
        if not regen_all:
            qs = qs.filter(thumbnail='')

        total = qs.count()
        if limit > 0:
            qs = qs[:limit]
            total = min(total, limit)

        self.stdout.write(f'Processing {total} photos...')

        created = 0
        skipped = 0
        failed = 0

        for photo in qs.iterator():
            if not photo.image:
                skipped += 1
                continue

            image_path = os.path.join(settings.MEDIA_ROOT, str(photo.image))
            if not os.path.isfile(image_path):
                skipped += 1
                continue

            thumb = generate_thumbnail_for_existing(image_path)
            if thumb is None:
                failed += 1
                continue

            photo.thumbnail.save(thumb.name, thumb, save=True)
            created += 1

            if created % 50 == 0:
                self.stdout.write(f'  ... {created}/{total}')

        self.stdout.write(self.style.SUCCESS(
            f'Done: {created} created, {skipped} skipped, {failed} failed'
        ))
