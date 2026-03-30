from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0011_rename_indexes_and_news_id'),
    ]

    operations = [
        migrations.AddField(
            model_name='advertphoto',
            name='thumbnail',
            field=models.FileField(blank=True, default='', upload_to='adverts/thumbs/'),
        ),
    ]
