# Heavy migration — split out from 0033 on purpose.
#
# `DistrictNdviSeries` has ~1.17M rows. Converting its `id` column from int4
# to int8 (BigAutoField) is a full table rewrite under an ACCESS EXCLUSIVE
# lock, which blocks reads/writes to the table (e.g. /api/ndvi-stats/) for the
# duration. It is isolated here so the rest of the drift cleanup (0033) stays
# instant and safe to auto-deploy, while this one can be applied deliberately
# in a low-traffic window if desired.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0033_rename_agro_yield__region__be041d_idx_agro_yield__region__fcf467_idx_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='districtndviseries',
            name='id',
            field=models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID'),
        ),
    ]
