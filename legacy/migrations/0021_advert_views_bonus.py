"""Разовый бонус: +10 просмотров каждому объявлению.

Счётчик просмотров = количество строк ``advert_view``, поэтому бонус
реализован как 10 служебных записей на объявление с ip ``0.0.0.0``
(без пользователя). Откат удаляет ровно эти записи.
"""
from django.db import migrations

BONUS_VIEWS = 10
BONUS_IP = '0.0.0.0'


def add_bonus_views(apps, schema_editor):
    Advert = apps.get_model('legacy', 'Advert')
    AdvertView = apps.get_model('legacy', 'AdvertView')
    rows = [
        AdvertView(advert_id=advert_id, ip_address=BONUS_IP)
        for advert_id in Advert.objects.values_list('id', flat=True)
        for _ in range(BONUS_VIEWS)
    ]
    AdvertView.objects.bulk_create(rows, batch_size=2000)


def remove_bonus_views(apps, schema_editor):
    AdvertView = apps.get_model('legacy', 'AdvertView')
    AdvertView.objects.filter(ip_address=BONUS_IP, user__isnull=True).delete()


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0020_emailcampaign_auto_send'),
    ]

    operations = [
        migrations.RunPython(add_bonus_views, remove_bonus_views),
    ]
