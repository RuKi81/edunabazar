"""
Performance indexes for the dashboard/report hot path.

- Partial index on VegetationIndex for ``index_type='ndvi' AND is_anomaly=false``
  (covers `api_ndvi_stats`, `api_report_region`, `api_report_district`, tile
  colouring, phenology pre-computation).
- Composite index on SatelliteScene ``(satellite, acquired_date)`` to accelerate
  JOINs driven by ``scene__satellite__in=(...)`` filters.

Both indexes are Postgres-only; project already pins PostGIS as the backend.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0014_add_mean_ndvi_to_phenology'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='satellitescene',
            index=models.Index(
                fields=['satellite', 'acquired_date'],
                name='scene_sat_date_idx',
            ),
        ),
        migrations.AddIndex(
            model_name='vegetationindex',
            index=models.Index(
                fields=['farmland', 'acquired_date'],
                condition=models.Q(index_type='ndvi', is_anomaly=False),
                name='vi_ndvi_active_idx',
            ),
        ),
    ]
