"""Переименовать каталоги фотографий объявлений: adverts/* -> listings/*.

Причина — generic-правило EasyList (используется uBlock Origin, AdGuard,
AdBlock Plus и др.):

    /adverts/*$~xmlhttprequest

Оно блокирует ЛЮБОЙ подресурс, в URL которого есть сегмент ``/adverts/``.
Миниатюры лежали по пути ``/media/adverts/thumbs/...``, поэтому у всех
пользователей с блокировщиком фотографии объявлений не загружались —
ни в списке, ни в карточке, ни на карте. HTML-страница ``/adverts/`` при
этом открывалась: top-level документы такими фильтрами не блокируются,
из-за чего баг выглядел как «вёрстка есть, фото нет».

Миграция переносит файлы на диске и синхронно правит пути в БД, поэтому
она обратима и не требует ручных действий при деплое. Старые URL
продолжают работать за счёт 301-редиректа в deploy/nginx.conf.
"""
import os

from django.conf import settings
from django.db import migrations, models
from django.db.models import Value
from django.db.models.functions import Replace


# (старый префикс, новый префикс) относительно MEDIA_ROOT
_MOVES = (
    ('adverts/photos', 'listings/photos'),
    ('adverts/thumbs', 'listings/thumbs'),
)


def _move_dir(old_rel: str, new_rel: str) -> None:
    """Перенести каталог внутри MEDIA_ROOT; молча выйти, если нечего нести."""
    media_root = str(getattr(settings, 'MEDIA_ROOT', '') or '')
    if not media_root:
        return
    old_dir = os.path.join(media_root, *old_rel.split('/'))
    new_dir = os.path.join(media_root, *new_rel.split('/'))
    if not os.path.isdir(old_dir):
        return
    os.makedirs(os.path.dirname(new_dir), exist_ok=True)
    if not os.path.isdir(new_dir):
        os.replace(old_dir, new_dir)
        return
    # Целевой каталог уже есть (повторный прогон / частичный перенос):
    # переносим файлы по одному, не перезатирая существующие.
    for name in os.listdir(old_dir):
        src = os.path.join(old_dir, name)
        dst = os.path.join(new_dir, name)
        if not os.path.exists(dst):
            os.replace(src, dst)


def _rewrite_paths(apps, old_rel: str, new_rel: str) -> None:
    """Заменить префикс пути в обеих файловых колонках."""
    AdvertPhoto = apps.get_model('legacy', 'AdvertPhoto')
    old_prefix = f'{old_rel}/'
    new_prefix = f'{new_rel}/'
    for field in ('image', 'thumbnail'):
        AdvertPhoto.objects.filter(**{f'{field}__startswith': old_prefix}).update(
            **{field: Replace(field, Value(old_prefix), Value(new_prefix))}
        )


def _drop_stale_caches() -> None:
    """Сбросить кэш выдачи: в нём лежит HTML со старыми URL картинок."""
    try:
        from legacy.cache_utils import invalidate_advert_caches, invalidate_home_cache

        invalidate_advert_caches()
        invalidate_home_cache()
    except Exception:
        # Кэш недоступен (тесты, локальный прогон) — не повод валить миграцию.
        pass


def forwards(apps, schema_editor):
    for old_rel, new_rel in _MOVES:
        _move_dir(old_rel, new_rel)
        _rewrite_paths(apps, old_rel, new_rel)
    _drop_stale_caches()


def backwards(apps, schema_editor):
    for old_rel, new_rel in _MOVES:
        _move_dir(new_rel, old_rel)
        _rewrite_paths(apps, new_rel, old_rel)
    _drop_stale_caches()


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0018_email_unsubscribe'),
    ]

    operations = [
        migrations.AlterField(
            model_name='advertphoto',
            name='image',
            field=models.FileField(upload_to='listings/photos/'),
        ),
        migrations.AlterField(
            model_name='advertphoto',
            name='thumbnail',
            field=models.FileField(blank=True, default='', upload_to='listings/thumbs/'),
        ),
        migrations.RunPython(forwards, backwards),
    ]
