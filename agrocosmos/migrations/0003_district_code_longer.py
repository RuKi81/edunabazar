from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0002_region_code_longer'),
    ]

    operations = [
        migrations.AlterField(
            model_name='district',
            name='code',
            field=models.CharField(blank=True, default='', max_length=150, verbose_name='Код района'),
        ),
    ]
