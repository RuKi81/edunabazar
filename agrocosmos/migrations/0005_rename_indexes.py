from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0004_monitoringtask'),
    ]

    operations = [
        migrations.RenameIndex(
            model_name='farmland',
            new_name='agro_farmla_distric_9bde9c_idx',
            old_name='agro_farmla_distric_idx',
        ),
        migrations.RenameIndex(
            model_name='vegetationindex',
            new_name='agro_vegeta_farmlan_c49b8b_idx',
            old_name='agro_vegidx_farm_type_date',
        ),
        migrations.RenameIndex(
            model_name='vegetationindex',
            new_name='agro_vegeta_acquire_6fe824_idx',
            old_name='agro_vegidx_date_type',
        ),
    ]
