"""Rename VegetationIndex.is_anomaly → is_outlier.

Old name was misleading: the flag marks a **spike in the time series**
(snow, cloud haze, SCL mask bleed-through) that must be excluded before
Savitzky-Golay smoothing. It is **not** a biological anomaly signal for
the agronomist — real vegetation stress alerts will live in a dedicated
``VegetationAlert`` table (future work).

The partial index ``vi_ndvi_active_idx`` references this column inside
its ``Q`` condition, so we must drop → rename → recreate.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0019_monitoringtask_raster_scope'),
    ]

    operations = [
        # ── 1. Drop the partial index that uses the old column name ──
        migrations.RemoveIndex(
            model_name='vegetationindex',
            name='vi_ndvi_active_idx',
        ),
        # ── 2. Rename the column (DB: ALTER TABLE ... RENAME COLUMN) ──
        migrations.RenameField(
            model_name='vegetationindex',
            old_name='is_anomaly',
            new_name='is_outlier',
        ),
        # ── 3. Update verbose_name to match new semantics ──
        migrations.AlterField(
            model_name='vegetationindex',
            name='is_outlier',
            field=models.BooleanField(
                default=False,
                verbose_name='Выброс (снег/облако, исключён из сглаживания)',
            ),
        ),
        # ── 4. Re-create the partial index with the new column name ──
        migrations.AddIndex(
            model_name='vegetationindex',
            index=models.Index(
                fields=['farmland', 'acquired_date'],
                condition=models.Q(index_type='ndvi', is_outlier=False),
                name='vi_ndvi_active_idx',
            ),
        ),
    ]
