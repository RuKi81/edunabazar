from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='region',
            name='code',
            field=models.CharField(max_length=100, unique=True, verbose_name='Код субъекта'),
        ),
    ]
