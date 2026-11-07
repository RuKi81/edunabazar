import datetime

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0019_filter_safe_media_paths'),
    ]

    operations = [
        migrations.AddField(
            model_name='emailcampaign',
            name='auto_send_enabled',
            field=models.BooleanField(default=False, verbose_name='Отправлять каждый день'),
        ),
        migrations.AddField(
            model_name='emailcampaign',
            name='auto_send_time',
            field=models.TimeField(default=datetime.time(9, 30), verbose_name='Время отправки (Москва)'),
        ),
        migrations.AddField(
            model_name='emailcampaign',
            name='auto_send_last_date',
            field=models.DateField(blank=True, null=True, verbose_name='Дата последней автоотправки'),
        ),
    ]
