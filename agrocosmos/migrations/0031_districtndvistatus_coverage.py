"""Add data-coverage counters to ``DistrictNdviStatus``.

For each district the choropleth pipeline now records, alongside the
weighted NDVI:

* ``farmlands_with_data`` — number of farmlands that contributed a valid
  VI row to the latest MODIS composite (i.e. count of rows that went
  into the area-weighted sum), and
* ``farmlands_total``    — total number of farmlands registered in the
  district at recompute time.

Their ratio is the "data trust" indicator surfaced in the admin-only
overlay on the all-Russia choropleth: when only 5 % of fields backed a
district's colour, the operator needs to know the number is fragile.

Both columns default to ``0`` so existing rows are valid until the next
``recompute_district_ndvi_status`` run repopulates them — at which
point the choropleth refresh also runs and the values become live.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0030_vegetation_alert_district_scope'),
    ]

    operations = [
        migrations.AddField(
            model_name='districtndvistatus',
            name='farmlands_with_data',
            field=models.IntegerField(
                default=0,
                verbose_name='Полей с данными на latest_date',
            ),
        ),
        migrations.AddField(
            model_name='districtndvistatus',
            name='farmlands_total',
            field=models.IntegerField(
                default=0,
                verbose_name='Всего полей в районе',
            ),
        ),
    ]
