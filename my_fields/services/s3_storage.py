"""Абстракция объектного хранилища (MinIO/S3) для растровых слоёв.

Тонкая обёртка над ``boto3``: presigned S3 Multipart Upload
(init/sign-part/complete/abort), presigned GET, GDAL ``/vsis3/`` путь,
head/delete объектов и генерация ключей.

Два эндпоинта (см. ``settings`` и ``deploy/minio/README.md``):

* **внутренний** (``S3_ENDPOINT_URL``) — серверные операции приложения:
  create/complete/abort multipart, head/delete, чтение COG. Идёт по
  приватной сети VM1→MinIO VM2.
* **публичный** (``S3_PUBLIC_ENDPOINT_URL``) — используется ТОЛЬКО для
  presigned URL, которые отдаются браузеру для прямой загрузки частей
  (``s3.edunabazar.ru`` через nginx).

Хранилище может быть не сконфигурировано (пустые ``S3_*``) — тогда
:func:`is_configured` вернёт ``False`` и вызывающий код обязан вернуть
понятную ошибку (растровый модуль выключен).

``boto3`` импортируется лениво внутри клиента, чтобы окружения без
объектного хранилища (и тесты на моках) не требовали пакет на импорте.
"""
from __future__ import annotations

import threading
import uuid
from urllib.parse import urlparse

from django.conf import settings

try:  # botocore ставится вместе с boto3; тесты на моках работают без него.
    from botocore.exceptions import ClientError
except ModuleNotFoundError:  # pragma: no cover - только окружения без S3
    class ClientError(Exception):  # type: ignore[no-redef]
        pass

# Кэш клиентов по ключу ('internal' | 'public'). boto3-клиент потокобезопасен
# для конкурентных вызовов, поэтому переиспользуем.
_clients: dict[str, object] = {}
_lock = threading.Lock()

# Дефолтный TTL presigned-ссылок (сек). Загрузка крупного файла долгая, но
# каждая ЧАСТЬ грузится отдельным PUT, поэтому часа с запасом достаточно.
DEFAULT_EXPIRES = 3600


def is_configured() -> bool:
    """True, если заданы эндпоинт и ключи доступа (модуль включён)."""
    return bool(
        getattr(settings, 'S3_ENDPOINT_URL', '')
        and getattr(settings, 'S3_ACCESS_KEY', '')
        and getattr(settings, 'S3_SECRET_KEY', '')
    )


def _make_client(endpoint_url: str):
    import boto3
    from botocore.config import Config

    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=getattr(settings, 'S3_REGION', 'us-east-1'),
        # MinIO работает в path-style (bucket в пути, не в поддомене);
        # s3v4 — обязателен для presigned upload_part.
        config=Config(signature_version='s3v4',
                      s3={'addressing_style': 'path'}),
    )


def _client(public: bool = False):
    """Ленивый кэшированный boto3-клиент (внутренний или публичный эндпоинт)."""
    key = 'public' if public else 'internal'
    client = _clients.get(key)
    if client is None:
        with _lock:
            client = _clients.get(key)
            if client is None:
                endpoint = (settings.S3_PUBLIC_ENDPOINT_URL if public
                            else settings.S3_ENDPOINT_URL)
                client = _make_client(endpoint)
                _clients[key] = client
    return client


def reset_clients() -> None:
    """Сбросить кэш клиентов (после смены настроек, напр. в тестах)."""
    with _lock:
        _clients.clear()


# ── Генерация ключей ─────────────────────────────────────────────────────

def build_upload_key(owner_id: int | None, filename: str) -> str:
    """Ключ оригинала в бакете загрузок: ``{owner|0}/{uuid}/original{.ext}``.

    Расширение берём из исходного имени (только для читаемости в консоли
    MinIO), сам файл идентифицируется uuid-папкой — коллизий нет.
    """
    ext = ''
    if '.' in filename:
        tail = filename.rsplit('.', 1)[1].lower()
        if tail.isalnum() and len(tail) <= 8:
            ext = '.' + tail
    return f'{owner_id or 0}/{uuid.uuid4().hex}/original{ext}'


def build_cog_key(layer_id: int) -> str:
    """Ключ COG в бакете ``S3_BUCKET_COG``: ``{layer_id}/cog.tif``."""
    return f'{layer_id}/cog.tif'


# ── Multipart upload (браузер → MinIO напрямую) ──────────────────────────

def create_multipart_upload(key: str, *, bucket: str | None = None,
                            content_type: str = 'image/tiff') -> str:
    """Инициировать multipart upload; вернуть ``UploadId``."""
    bucket = bucket or settings.S3_BUCKET_UPLOADS
    resp = _client().create_multipart_upload(
        Bucket=bucket, Key=key, ContentType=content_type)
    return resp['UploadId']


def presign_part_url(key: str, upload_id: str, part_number: int, *,
                     bucket: str | None = None,
                     expires: int = DEFAULT_EXPIRES) -> str:
    """Presigned URL для PUT одной части (отдаётся браузеру, публичный хост)."""
    bucket = bucket or settings.S3_BUCKET_UPLOADS
    return _client(public=True).generate_presigned_url(
        'upload_part',
        Params={'Bucket': bucket, 'Key': key,
                'UploadId': upload_id, 'PartNumber': int(part_number)},
        ExpiresIn=expires,
    )


def complete_multipart_upload(key: str, upload_id: str, parts: list[dict], *,
                              bucket: str | None = None) -> dict:
    """Финализировать multipart upload.

    ``parts`` — список ``{'PartNumber': int, 'ETag': str}`` (порядок не важен,
    сортируется здесь).
    """
    bucket = bucket or settings.S3_BUCKET_UPLOADS
    ordered = sorted(
        ({'PartNumber': int(p['PartNumber']), 'ETag': p['ETag']} for p in parts),
        key=lambda p: p['PartNumber'],
    )
    return _client().complete_multipart_upload(
        Bucket=bucket, Key=key, UploadId=upload_id,
        MultipartUpload={'Parts': ordered},
    )


def abort_multipart_upload(key: str, upload_id: str, *,
                           bucket: str | None = None) -> None:
    """Отменить multipart upload (освобождает залитые части)."""
    bucket = bucket or settings.S3_BUCKET_UPLOADS
    _client().abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)


# ── Объекты ──────────────────────────────────────────────────────────────

def head_object(key: str, *, bucket: str | None = None) -> dict | None:
    """Метаданные объекта (``ContentLength`` и т.д.) или ``None`` если нет."""
    bucket = bucket or settings.S3_BUCKET_UPLOADS
    try:
        return _client().head_object(Bucket=bucket, Key=key)
    except ClientError:
        return None


def object_size(key: str, *, bucket: str | None = None) -> int | None:
    """Размер объекта в байтах или ``None`` если объект отсутствует."""
    head = head_object(key, bucket=bucket)
    return int(head['ContentLength']) if head else None


def delete_object(key: str, *, bucket: str | None = None) -> None:
    """Удалить объект (idempotent — S3 не ошибается на отсутствующем ключе)."""
    if not key:
        return
    bucket = bucket or settings.S3_BUCKET_UPLOADS
    _client().delete_object(Bucket=bucket, Key=key)


def download_object(key: str, dest_path: str, *, bucket: str | None = None) -> None:
    """Скачать объект в локальный файл ``dest_path`` (для конвейера ingest)."""
    bucket = bucket or settings.S3_BUCKET_UPLOADS
    _client().download_file(bucket, key, dest_path)


def upload_file(src_path: str, key: str, *, bucket: str | None = None,
                content_type: str = 'image/tiff') -> None:
    """Залить локальный файл ``src_path`` в объект ``key`` (напр. готовый COG)."""
    bucket = bucket or settings.S3_BUCKET_COG
    _client().upload_file(
        src_path, bucket, key, ExtraArgs={'ContentType': content_type})


def presign_get_url(key: str, *, bucket: str | None = None,
                    expires: int = DEFAULT_EXPIRES, public: bool = True) -> str:
    """Presigned URL для GET объекта (напр. скачать оригинал/COG)."""
    bucket = bucket or settings.S3_BUCKET_COG
    return _client(public=public).generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': key},
        ExpiresIn=expires,
    )


# ── GDAL /vsis3/ (чтение COG по range-запросам) ──────────────────────────

def vsis3_path(key: str, *, bucket: str | None = None) -> str:
    """GDAL-путь ``/vsis3/{bucket}/{key}`` для чтения COG через rasterio."""
    bucket = bucket or settings.S3_BUCKET_COG
    return f'/vsis3/{bucket}/{key}'


def gdal_vsis3_env() -> dict:
    """Kwargs для ``rasterio.Env`` для чтения ``/vsis3/`` из MinIO.

    Применяются в рендерере тайлов (Фаза 4) через ``rasterio.Env(**env)``.

    ВАЖНО: креды передаются через :class:`rasterio.session.AWSSession` (boto3),
    а НЕ как GDAL-опции — rasterio запрещает ``AWS_ACCESS_KEY_ID`` /
    ``AWS_SECRET_ACCESS_KEY`` прямо в ``Env`` (``EnvError``). Остальное —
    GDAL-настройки: ``AWS_S3_ENDPOINT`` (host[:port] без схемы),
    ``AWS_HTTPS`` + ``AWS_VIRTUAL_HOSTING`` (http, path-style для MinIO), кэш.
    """
    from rasterio.session import AWSSession

    parsed = urlparse(settings.S3_ENDPOINT_URL)
    host = parsed.netloc or parsed.path  # на случай "host:port" без схемы
    session = AWSSession(
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=getattr(settings, 'S3_REGION', 'us-east-1'),
    )
    return {
        'session': session,
        'AWS_S3_ENDPOINT': host,
        'AWS_HTTPS': 'YES' if parsed.scheme == 'https' else 'NO',
        'AWS_VIRTUAL_HOSTING': 'FALSE',       # path-style (MinIO)
        # Ускоряет открытие COG: не листить «директорию» бакета.
        'GDAL_DISABLE_READDIR_ON_OPEN': 'EMPTY_DIR',
        'CPL_VSIL_CURL_ALLOWED_EXTENSIONS': '.tif',
        'VSI_CACHE': 'TRUE',
    }
