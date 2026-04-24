"""Add `osm_id` to Region and District for direct OSM relation linkage.

Enables per-region Overpass queries during district bulk-import (avoids
the 15-min QL timeout on a country-wide admin_level=6 query) and lets
us upsert Districts deterministically by their OSM relation id rather
than relying on (region, name) which isn't unique (same district name
repeats across subjects).
"""
from django.db import migrations, models


def _backfill_osm_ids(apps, schema_editor):
    """Recover osm_id from rows whose `code` was saved as 'osm_<id>'.

    Districts imported before the osm_id column existed have
    code='osm_1574582' etc. — we can trivially map those back so the
    next import deduplicates on osm_id instead of creating copies.
    Region rows with non-osm codes (e.g. 'krim_resp', 'RU-KDA') are
    left untouched here; refresh them later via
    `import_russia_regions --refresh-osm-ids`.
    """
    for model_name in ('region', 'district'):
        Model = apps.get_model('agrocosmos', model_name)
        for row in Model.objects.filter(code__startswith='osm_', osm_id__isnull=True):
            try:
                row.osm_id = int(row.code.split('_', 1)[1])
            except (ValueError, IndexError):
                continue
            row.save(update_fields=['osm_id'])


def _noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0024_pipelinerun_queue'),
    ]

    operations = [
        migrations.AddField(
            model_name='region',
            name='osm_id',
            field=models.BigIntegerField(
                null=True, blank=True, unique=True, db_index=True,
                verbose_name='OSM relation id',
            ),
        ),
        migrations.AddField(
            model_name='district',
            name='osm_id',
            field=models.BigIntegerField(
                null=True, blank=True, unique=True, db_index=True,
                verbose_name='OSM relation id',
            ),
        ),
        migrations.RunPython(_backfill_osm_ids, _noop_reverse),
    ]
