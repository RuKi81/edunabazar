"""Housekeeping after 0026:

* Django 5.1 changed the auto-name algorithm for ``Index``es — the
  legacy hash-suffixed names drift to new hashes. ``RenameIndex`` is
  a metadata-only operation (no rewrite), so we accept the churn.
* ``NdviBaseline.crop_type`` reuses ``Farmland.CropType.choices`` —
  0026 added ``fallow`` and ``other_agri``, so Django wants an
  ``AlterField`` on this reference too (choices are Python-level,
  no SQL change).

Produced by ``makemigrations --dry-run`` after 0026 was applied
(verbatim from the server output).
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0026_farmland_zsn_expand'),
    ]

    operations = [
        migrations.RenameIndex(
            model_name='agrosubscription',
            old_name='agro_subscr_legacy__a8b0df_idx',
            new_name='agro_subscr_legacy__2f129e_idx',
        ),
        migrations.RenameIndex(
            model_name='agrosubscription',
            old_name='agro_subscr_region__b8a5b6_idx',
            new_name='agro_subscr_region__c4772f_idx',
        ),
        migrations.RenameIndex(
            model_name='farmland',
            old_name='agro_farmla_region__cropt_idx',
            new_name='agro_farmla_region__911936_idx',
        ),
        migrations.RenameIndex(
            model_name='farmland',
            old_name='agro_farmla_region__isused_idx',
            new_name='agro_farmla_region__fd253c_idx',
        ),
        migrations.RenameIndex(
            model_name='vegetationalert',
            old_name='agro_vegeta_farmlan_e65097_idx',
            new_name='agro_vegeta_farmlan_e6fa0b_idx',
        ),
        migrations.AlterField(
            model_name='ndvibaseline',
            name='crop_type',
            field=models.CharField(
                max_length=20,
                blank=True,
                default='',
                choices=[
                    ('arable', 'Пашня'),
                    ('fallow', 'Залежь'),
                    ('hayfield', 'Сенокос'),
                    ('pasture', 'Пастбище'),
                    ('perennial', 'Многолетние насаждения'),
                    ('other_agri', 'Иные с.-х. земли'),
                    ('other', 'Прочее'),
                ],
                verbose_name='Вид угодья (пусто = все)',
            ),
        ),
    ]
