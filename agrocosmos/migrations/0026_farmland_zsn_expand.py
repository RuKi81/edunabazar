"""Expand ``Farmland`` for the Rosreestr ЗСН bulk-import.

Changes:
    * CropType enum gets ``fallow`` and ``other_agri``.
    * ``region`` FK added (denormalises ``district.region`` for fast
      per-region queries and lets us insert rows before a matching
      district is known).
    * ``district`` FK made nullable (spatial-join may miss a district;
      we don't want to drop the polygon).
    * ``is_used`` BooleanField added (nullable tri-state) for the
      ``Fact_isp`` attribute present in ~28 regions.
    * ``cadastral_number`` gains a btree index (queried by value).
    * ``source`` CharField added to track the originating dataset /
      schema id so we can re-import or upgrade per source.
    * Index set reshuffled to the triples we actually filter by.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0025_region_district_osm_id'),
    ]

    operations = [
        # --- make district FK nullable -----------------------------------
        migrations.AlterField(
            model_name='farmland',
            name='district',
            field=models.ForeignKey(
                blank=True, null=True,
                on_delete=models.deletion.SET_NULL,
                related_name='farmlands',
                to='agrocosmos.district',
                verbose_name='Район',
            ),
        ),
        # --- new FK: region ----------------------------------------------
        migrations.AddField(
            model_name='farmland',
            name='region',
            field=models.ForeignKey(
                blank=True, null=True,
                on_delete=models.deletion.SET_NULL,
                related_name='farmlands',
                to='agrocosmos.region',
                verbose_name='Регион',
            ),
        ),
        # --- is_used (Fact_isp) ------------------------------------------
        migrations.AddField(
            model_name='farmland',
            name='is_used',
            field=models.BooleanField(
                null=True, blank=True,
                verbose_name='Факт использования',
                help_text='True — используется, False — не используется, NULL — неизвестно',
            ),
        ),
        # --- source (schema id) ------------------------------------------
        migrations.AddField(
            model_name='farmland',
            name='source',
            field=models.CharField(
                max_length=40, blank=True, default='',
                verbose_name='Источник (schema_id)',
                help_text='Идентификатор исходной схемы данных, например "rosreestr_zsn/altai"',
            ),
        ),
        # --- crop_type: add new choices (DB column unchanged) ------------
        migrations.AlterField(
            model_name='farmland',
            name='crop_type',
            field=models.CharField(
                max_length=20,
                default='arable',
                verbose_name='Вид угодья',
                choices=[
                    ('arable', 'Пашня'),
                    ('fallow', 'Залежь'),
                    ('hayfield', 'Сенокос'),
                    ('pasture', 'Пастбище'),
                    ('perennial', 'Многолетние насаждения'),
                    ('other_agri', 'Иные с.-х. земли'),
                    ('other', 'Прочее'),
                ],
            ),
        ),
        # --- cadastral_number: add db_index ------------------------------
        migrations.AlterField(
            model_name='farmland',
            name='cadastral_number',
            field=models.CharField(
                max_length=50, blank=True, default='', db_index=True,
                verbose_name='Кадастровый номер',
            ),
        ),
        # --- reorder default ordering ------------------------------------
        migrations.AlterModelOptions(
            name='farmland',
            options={
                'db_table': 'agro_farmland',
                'ordering': ['region', 'district', 'crop_type'],
                'verbose_name': 'Угодье',
                'verbose_name_plural': 'Угодья',
            },
        ),
        # --- index set ---------------------------------------------------
        migrations.AddIndex(
            model_name='farmland',
            index=models.Index(
                fields=['region', 'crop_type'],
                name='agro_farmla_region__cropt_idx',
            ),
        ),
        migrations.AddIndex(
            model_name='farmland',
            index=models.Index(
                fields=['region', 'is_used'],
                name='agro_farmla_region__isused_idx',
            ),
        ),
    ]
