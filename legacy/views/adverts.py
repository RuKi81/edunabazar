import urllib.parse

from django.contrib.postgres.search import SearchQuery, SearchRank
from django.core.paginator import Paginator
from django.db.models import Prefetch
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.contrib.gis.geos import Point
from PIL import Image as PILImage

from ..models import (
    Advert, AdvertPhoto, Catalog, Categories, Review, Favorite, AdvertView,
)
from ..cache_utils import invalidate_advert_caches
from ..constants import (
    ADVERT_STATUS_DELETED, ADVERT_STATUS_HIDDEN, ADVERT_STATUS_MODERATION,
    ADVERT_STATUS_PUBLISHED,
)
from ..slug_utils import get_slug_map
from ..image_utils import process_uploaded_image
from .helpers import (
    _get_current_legacy_user, _is_admin_user, _get_admin_identity,
    _can_edit_advert, _can_manage_advert,
    _no_store, _update_advert_status, _normalize_extra_contacts,
    _send_advert_published_email, logger,
)
from .reviews import _get_reviews, _avg_points


def advert_list(request: HttpRequest, catalog_slug: str = '', category_slug: str = '') -> HttpResponse:
    legacy_user = _get_current_legacy_user(request)

    # --- Resolve catalog/category from slug or GET params ---
    slug_map = get_slug_map()
    catalog_id = None
    category_id = None

    if catalog_slug:
        catalog_id = slug_map['catalog_by_slug'].get(catalog_slug)
        if catalog_id is None:
            raise Http404
        if category_slug:
            category_id = slug_map['category_by_slug'].get((catalog_slug, category_slug))
            if category_id is None:
                raise Http404
    else:
        # Legacy GET params — redirect to slug URL if possible
        try:
            _cat_id = int((request.GET.get('catalog') or '').strip() or 0) or None
        except Exception:
            _cat_id = None
        try:
            _categ_id = int((request.GET.get('category') or '').strip() or 0) or None
        except Exception:
            _categ_id = None

        if _categ_id and _categ_id in slug_map['category_by_id']:
            cs, cats = slug_map['category_by_id'][_categ_id]
            params = request.GET.copy()
            params.pop('catalog', None)
            params.pop('category', None)
            qs_str = params.urlencode()
            url = f'/adverts/{cs}/{cats}/'
            if qs_str:
                url += '?' + qs_str
            return redirect(url, permanent=True)
        elif _cat_id and _cat_id in slug_map['catalog_by_id']:
            cs = slug_map['catalog_by_id'][_cat_id]
            params = request.GET.copy()
            params.pop('catalog', None)
            qs_str = params.urlencode()
            url = f'/adverts/{cs}/'
            if qs_str:
                url += '?' + qs_str
            return redirect(url, permanent=True)
        else:
            catalog_id = _cat_id
            category_id = _categ_id

    q = (request.GET.get('q') or '').strip()
    sort = (request.GET.get('sort') or 'id').strip()
    if sort not in {'id', 'price', 'count'}:
        sort = 'id'

    page_size_raw = (request.GET.get('page_size') or '12').strip()
    try:
        page_size = int(page_size_raw)
    except (TypeError, ValueError):
        page_size = 12
    if page_size not in {12, 24, 36, 48}:
        page_size = 12

    _thumb_prefetch = Prefetch(
        'photos',
        queryset=AdvertPhoto.objects.order_by('sort', 'id'),
        to_attr='prefetched_photos',
    )
    qs = Advert.objects.select_related('category', 'author').prefetch_related(_thumb_prefetch)
    if _is_admin_user(legacy_user):
        qs = qs.exclude(status=ADVERT_STATUS_DELETED)
    else:
        qs = qs.filter(status=ADVERT_STATUS_PUBLISHED)
    search_query = None
    if q:
        search_query = SearchQuery(q, config='russian')
        qs = qs.extra(where=["search_vector @@ plainto_tsquery('russian', %s)"], params=[q])
        qs = qs.extra(select={'_rank': "ts_rank(search_vector, plainto_tsquery('russian', %s))"}, select_params=[q])

    type_raw = (request.GET.get('type') or '').strip().lower()
    if type_raw == 'offer':
        qs = qs.filter(type=0)
    elif type_raw == 'demand':
        qs = qs.filter(type=1)

    opt_raw = (request.GET.get('opt') or '').strip().lower()
    if opt_raw in {'1', 'true', 'yes', 'on'}:
        qs = qs.filter(wholesale_price__gt=0)

    delivery_raw = (request.GET.get('delivery') or '').strip().lower()
    if delivery_raw in {'1', 'true', 'yes', 'on'}:
        qs = qs.filter(delivery=True)

    if category_id is not None:
        qs = qs.filter(category_id=category_id)
    if catalog_id is not None:
        qs = qs.filter(category__catalog_id=catalog_id)

    if q and sort == 'id':
        qs = qs.order_by('-_rank', '-created_at', '-id')
    elif sort == 'price':
        qs = qs.order_by('-price', '-created_at', '-id')
    elif sort == 'count':
        qs = qs.order_by('-priority', '-created_at', '-id')
    else:
        qs = qs.order_by('-created_at', '-id')

    paginator = Paginator(qs, page_size)
    adverts_page = paginator.get_page(request.GET.get('page') or 1)
    page_range = paginator.get_elided_page_range(adverts_page.number)

    resp = render(
        request,
        'legacy/advert_list.html',
        {
            'adverts': adverts_page,
            'sort': sort,
            'page_size': page_size,
            'page_range': page_range,
            'catalogs': Catalog.objects.filter(active=1).order_by('sort', 'title', 'id'),
            'catalog_id': catalog_id,
            'categories': Categories.objects.filter(active=1).select_related('catalog').order_by('title'),
            'category_id': category_id,
            'legacy_user': legacy_user,
            'catalog_slug': catalog_slug,
            'category_slug': category_slug,
            'slug_map': slug_map,
        },
    )
    return _no_store(resp)


def advert_detail(request: HttpRequest, advert_id: int) -> HttpResponse:
    advert = get_object_or_404(Advert.objects.select_related('category__catalog', 'author'), pk=advert_id)
    photos = list(AdvertPhoto.objects.filter(advert_id=int(advert_id)).order_by('sort', 'id'))
    admin_identity = _get_admin_identity(request)
    can_manage = _can_manage_advert(admin_identity, advert)
    can_edit = _can_edit_advert(admin_identity, advert)

    advert_lat = None
    advert_lon = None
    try:
        loc = getattr(advert, 'location', None)
        if loc:
            advert_lat = float(loc.y)
            advert_lon = float(loc.x)
    except Exception:
        pass

    advert_address = (getattr(advert, 'address', '') or '').strip()
    show_address = bool(advert_lat is not None or advert_address)

    legacy_user = _get_current_legacy_user(request)
    is_admin = _is_admin_user(legacy_user)
    reviews_qs = _get_reviews(Review.REVIEW_TYPE_ADVERT, advert_id, include_moderation=is_admin)
    reviews_list = list(reviews_qs[:50])
    avg_rating = _avg_points(reviews_list)

    review_error = request.session.pop('review_error', '')
    review_success = request.session.pop('review_success', '')

    # Track view (once per IP per day)
    ip = request.META.get('HTTP_X_REAL_IP') or request.META.get('REMOTE_ADDR') or ''
    today = timezone.now().date()
    already_viewed = AdvertView.objects.filter(
        advert_id=advert_id, ip_address=ip, created_at__date=today,
    ).exists()
    if not already_viewed:
        AdvertView.objects.create(
            advert_id=advert_id, ip_address=ip,
            user=legacy_user if legacy_user else None,
        )
    view_count = AdvertView.objects.filter(advert_id=advert_id).count()

    # Favorite status
    is_favorited = False
    if legacy_user:
        is_favorited = Favorite.objects.filter(user=legacy_user, advert_id=advert_id).exists()

    # Similar adverts (same category, excluding current)
    _thumb_prefetch = Prefetch(
        'photos',
        queryset=AdvertPhoto.objects.order_by('sort', 'id'),
        to_attr='prefetched_photos',
    )
    similar_adverts = list(
        Advert.objects.filter(
            category_id=advert.category_id, status=ADVERT_STATUS_PUBLISHED,
        ).exclude(pk=advert_id)
        .select_related('category')
        .prefetch_related(_thumb_prefetch)
        .order_by('-created_at')[:6]
    )

    # Other adverts by the same seller
    seller_adverts = list(
        Advert.objects.filter(
            author_id=advert.author_id, status=ADVERT_STATUS_PUBLISHED,
        ).exclude(pk=advert_id)
        .select_related('category')
        .prefetch_related(_thumb_prefetch)
        .order_by('-created_at')[:6]
    )

    resp = render(
        request,
        'legacy/advert_detail.html',
        {
            'advert': advert,
            'photos': photos,
            'legacy_user': legacy_user,
            'can_manage_advert': can_manage,
            'can_edit_advert': can_edit,
            'show_address': show_address,
            'advert_lat': advert_lat,
            'advert_lon': advert_lon,
            'advert_address': advert_address,
            'reviews': reviews_list,
            'avg_rating': avg_rating,
            'review_error': review_error,
            'review_success': review_success,
            'review_type': Review.REVIEW_TYPE_ADVERT,
            'review_object_id': advert_id,
            'is_admin_user': is_admin,
            'view_count': view_count,
            'is_favorited': is_favorited,
            'extra_contacts': _normalize_extra_contacts(getattr(advert, 'extra_contacts', None) or []),
            'similar_adverts': similar_adverts,
            'seller_adverts': seller_adverts,
        },
    )
    return _no_store(resp)


def _parse_advert_form(post, files):
    """Parse and validate advert form fields. Returns (cleaned, errors, form_data)."""
    type_raw = (post.get('type') or '0').strip()
    try:
        advert_type = int(type_raw)
        if advert_type not in {0, 1}:
            advert_type = 0
    except Exception:
        advert_type = 0

    category_raw = (post.get('category') or '').strip()
    title = (post.get('title') or '').strip()
    text = (post.get('text') or '').strip()
    contacts = (post.get('contacts') or '').strip()
    price_raw = (post.get('price') or '0').strip().replace(',', '.')
    price_unit = (post.get('price_unit') or 'кг').strip()
    volume_raw = (post.get('volume') or '0').strip().replace(',', '.')
    min_volume_raw = (post.get('min_volume') or '0').strip().replace(',', '.')
    wholesale_volume_raw = (post.get('wholesale_volume') or '0').strip().replace(',', '.')
    opt_raw = (post.get('opt') or '').strip().lower()
    delivery_raw = (post.get('delivery') or '').strip().lower()
    lat_raw = (post.get('lat') or '').strip().replace(',', '.')
    lon_raw = (post.get('lon') or '').strip().replace(',', '.')
    address = (post.get('address') or '').strip()

    errors = {}

    if not title:
        errors['title'] = 'Введите заголовок'
    if not text:
        errors['text'] = 'Введите описание'
    if not contacts:
        errors['contacts'] = 'Введите контактный телефон'
    if not address:
        errors['address'] = 'Введите адрес'

    category_id = None
    if not category_raw:
        errors['category'] = 'Выберите категорию'
    else:
        try:
            category_id = int(category_raw)
            if not Categories.objects.filter(pk=category_id, active=1).exists():
                errors['category'] = 'Неверная категория'
                category_id = None
        except Exception:
            errors['category'] = 'Неверная категория'

    try:
        price = max(0.0, float(price_raw))
    except Exception:
        price = 0.0

    try:
        volume = max(0.0, float(volume_raw))
    except Exception:
        volume = 0.0

    try:
        min_volume = max(0.0, float(min_volume_raw))
    except Exception:
        min_volume = 0.0

    try:
        wholesale_volume = max(0.0, float(wholesale_volume_raw))
    except Exception:
        wholesale_volume = 0.0

    is_opt = opt_raw in {'1', 'true', 'on', 'yes'}
    wholesale_price = price if is_opt else 0.0
    delivery = delivery_raw in {'1', 'true', 'on', 'yes'}

    lat = None
    lon = None
    try:
        if lat_raw and lon_raw:
            lat = float(lat_raw)
            lon = float(lon_raw)
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                lat = None
                lon = None
    except Exception:
        lat = None
        lon = None

    if lat is None or lon is None:
        errors['lat'] = 'Укажите адрес на карте'

    _CONTACT_TYPES = {'email', 'telegram', 'max', 'social', 'website'}
    extra_contacts = []
    ec_types = post.getlist('ec_type') if hasattr(post, 'getlist') else []
    ec_values = post.getlist('ec_value') if hasattr(post, 'getlist') else []
    for ct, cv in zip(ec_types, ec_values):
        ct = (ct or '').strip()
        cv = (cv or '').strip()
        if ct in _CONTACT_TYPES and cv:
            extra_contacts.append({'type': ct, 'value': cv})

    photos = files.getlist('photos') if files else []
    if len(photos) > 10:
        errors['photos'] = 'Максимум 10 фотографий'

    _ALLOWED_IMAGE_TYPES = {'image/jpeg', 'image/png', 'image/gif', 'image/webp'}
    _MAX_PHOTO_SIZE = 10 * 1024 * 1024  # 10 MB
    for i, photo in enumerate(photos):
        ct = getattr(photo, 'content_type', '') or ''
        size = getattr(photo, 'size', 0) or 0
        if ct not in _ALLOWED_IMAGE_TYPES:
            errors['photos'] = f'Файл «{photo.name}» — недопустимый формат. Разрешены: JPEG, PNG, GIF, WebP'
            break
        if size > _MAX_PHOTO_SIZE:
            errors['photos'] = f'Файл «{photo.name}» слишком большой (макс. 10 МБ)'
            break
        try:
            photo.seek(0)
            img = PILImage.open(photo)
            img.verify()
            photo.seek(0)
        except Exception:
            errors['photos'] = f'Файл «{photo.name}» повреждён или не является изображением'
            break

    cleaned = {
        'type': advert_type,
        'category_id': category_id,
        'title': title,
        'text': text,
        'contacts': contacts,
        'address': address,
        'price': price,
        'price_unit': price_unit,
        'volume': volume,
        'min_volume': min_volume,
        'wholesale_volume': wholesale_volume,
        'wholesale_price': wholesale_price,
        'delivery': delivery,
        'lat': lat,
        'lon': lon,
        'photos': photos,
        'extra_contacts': extra_contacts,
    }
    form_data = {
        'type': advert_type,
        'catalog': (post.get('catalog') or ''),
        'category': category_id or category_raw,
        'title': title,
        'text': text,
        'contacts': contacts,
        'address': address,
        'price': price_raw,
        'price_unit': price_unit,
        'volume': volume_raw,
        'min_volume': min_volume_raw,
        'wholesale_volume': wholesale_volume_raw,
        'opt': is_opt,
        'delivery': delivery,
        'lat': lat_raw,
        'lon': lon_raw,
        'extra_contacts': extra_contacts,
    }
    return cleaned, errors, form_data


def advert_create(request: HttpRequest) -> HttpResponse:
    user = _get_current_legacy_user(request)
    if not user:
        return redirect(f"/login/?next={urllib.parse.quote(request.get_full_path())}")

    errors: dict = {}
    form_data: dict = {}

    if request.method == 'POST':
        cleaned, errors, form_data = _parse_advert_form(request.POST, request.FILES)
        if not errors:
            now = timezone.now()
            advert = Advert.objects.create(
                type=cleaned['type'],
                category_id=cleaned['category_id'],
                author_id=int(user.id),
                address=cleaned['address'],
                location=Point(float(cleaned['lon']), float(cleaned['lat']), srid=4326),
                delivery=cleaned['delivery'],
                contacts=cleaned['contacts'],
                title=cleaned['title'],
                text=cleaned['text'],
                price=cleaned['price'],
                price_unit=cleaned['price_unit'],
                wholesale_price=cleaned['wholesale_price'],
                min_volume=cleaned['min_volume'],
                wholesale_volume=cleaned['wholesale_volume'],
                volume=cleaned['volume'],
                extra_contacts=cleaned['extra_contacts'] or [],
                priority=0,
                created_at=now,
                updated_at=now,
                status=ADVERT_STATUS_MODERATION,
            )
            for i, photo_file in enumerate(cleaned['photos']):
                compressed, thumb = process_uploaded_image(photo_file)
                photo = AdvertPhoto(advert=advert, sort=i)
                photo.image.save(compressed.name, compressed, save=False)
                if thumb:
                    photo.thumbnail.save(thumb.name, thumb, save=False)
                photo.save()
            invalidate_advert_caches()
            return redirect(f"/adverts/{int(advert.id)}/")

    resp = render(
        request,
        'legacy/advert_create.html',
        {
            'legacy_user': user,
            'catalogs': Catalog.objects.filter(active=1).order_by('sort', 'title', 'id'),
            'categories': Categories.objects.filter(active=1).select_related('catalog').order_by('title'),
            'errors': errors,
            'form': form_data,
        },
    )
    return _no_store(resp)


def advert_edit(request: HttpRequest, advert_id: int) -> HttpResponse:
    user = _get_current_legacy_user(request)
    if not user:
        return redirect(f"/login/?next={urllib.parse.quote(request.get_full_path())}")
    advert = get_object_or_404(Advert.objects.select_related('category', 'author'), pk=advert_id)
    if not _can_edit_advert(_get_admin_identity(request), advert):
        return redirect(f"/adverts/{int(advert_id)}/")

    errors: dict = {}
    existing_photos = list(AdvertPhoto.objects.filter(advert_id=int(advert_id)).order_by('sort', 'id'))

    if request.method == 'POST':
        cleaned, errors, form_data = _parse_advert_form(request.POST, request.FILES)
        if not errors:
            Advert.objects.filter(pk=int(advert_id)).update(
                type=cleaned['type'],
                category_id=cleaned['category_id'],
                address=cleaned['address'],
                location=Point(float(cleaned['lon']), float(cleaned['lat']), srid=4326),
                delivery=cleaned['delivery'],
                contacts=cleaned['contacts'],
                title=cleaned['title'],
                text=cleaned['text'],
                price=cleaned['price'],
                price_unit=cleaned['price_unit'],
                wholesale_price=cleaned['wholesale_price'],
                min_volume=cleaned['min_volume'],
                wholesale_volume=cleaned['wholesale_volume'],
                volume=cleaned['volume'],
                extra_contacts=cleaned['extra_contacts'] or [],
                updated_at=timezone.now(),
            )
            delete_ids_raw = request.POST.getlist('delete_photo')
            delete_ids = []
            for rid in delete_ids_raw:
                try:
                    delete_ids.append(int(rid))
                except Exception:
                    pass
            if delete_ids:
                AdvertPhoto.objects.filter(advert_id=int(advert_id), id__in=delete_ids).delete()
            next_sort = AdvertPhoto.objects.filter(advert_id=int(advert_id)).count()
            for i, photo_file in enumerate(cleaned['photos']):
                compressed, thumb = process_uploaded_image(photo_file)
                photo = AdvertPhoto(advert=advert, sort=next_sort + i)
                photo.image.save(compressed.name, compressed, save=False)
                if thumb:
                    photo.thumbnail.save(thumb.name, thumb, save=False)
                photo.save()
            invalidate_advert_caches()
            return redirect(f"/adverts/{int(advert_id)}/")
    else:
        try:
            loc = getattr(advert, 'location', None)
            lat_val = float(loc.y) if loc else ''
            lon_val = float(loc.x) if loc else ''
        except Exception:
            lat_val = ''
            lon_val = ''
        form_data = {
            'type': getattr(advert, 'type', 0),
            'catalog': getattr(advert.category, 'catalog_id', '') if advert.category_id else '',
            'category': advert.category_id or '',
            'title': advert.title or '',
            'text': advert.text or '',
            'contacts': advert.contacts or '',
            'address': advert.address or '',
            'price': advert.price or '',
            'price_unit': getattr(advert, 'price_unit', 'кг') or 'кг',
            'volume': advert.volume or '',
            'min_volume': advert.min_volume or '',
            'wholesale_volume': advert.wholesale_volume or '',
            'opt': bool((advert.wholesale_price or 0) > 0),
            'delivery': bool(advert.delivery),
            'lat': lat_val,
            'lon': lon_val,
            'extra_contacts': getattr(advert, 'extra_contacts', None) or [],
        }

    resp = render(
        request,
        'legacy/advert_edit.html',
        {
            'legacy_user': user,
            'advert': advert,
            'existing_photos': existing_photos,
            'errors': errors,
            'form': form_data,
            'catalogs': Catalog.objects.filter(active=1).order_by('sort', 'title', 'id'),
            'categories': Categories.objects.filter(active=1).select_related('catalog').order_by('title'),
        },
    )
    return _no_store(resp)


def advert_hide(request: HttpRequest, advert_id: int) -> HttpResponse:
    if request.method != 'POST':
        return redirect(f"/adverts/{int(advert_id)}/")
    advert = get_object_or_404(Advert, pk=advert_id)
    if not _can_manage_advert(_get_admin_identity(request), advert):
        return redirect(f"/adverts/{int(advert_id)}/")
    _update_advert_status(int(advert_id), ADVERT_STATUS_HIDDEN)
    return redirect(f"/adverts/{int(advert_id)}/")


def advert_publish(request: HttpRequest, advert_id: int) -> HttpResponse:
    if request.method != 'POST':
        return redirect(f"/adverts/{int(advert_id)}/")
    advert = get_object_or_404(Advert.objects.select_related('author'), pk=advert_id)
    if not _can_manage_advert(_get_admin_identity(request), advert):
        return redirect(f"/adverts/{int(advert_id)}/")
    _update_advert_status(int(advert_id), ADVERT_STATUS_PUBLISHED)

    try:
        author = getattr(advert, 'author', None)
        author_email = (getattr(author, 'email', None) or '').strip()
        if author_email:
            url = request.build_absolute_uri(f"/adverts/{int(advert_id)}/")
            _send_advert_published_email(author_email, getattr(advert, 'title', '') or '', url)
    except Exception:
        pass

    return redirect(f"/adverts/{int(advert_id)}/")


def advert_bump(request: HttpRequest, advert_id: int) -> HttpResponse:
    if request.method != 'POST':
        return redirect(f"/adverts/{int(advert_id)}/")
    advert = get_object_or_404(Advert, pk=advert_id)
    if not _can_manage_advert(_get_admin_identity(request), advert):
        return redirect(f"/adverts/{int(advert_id)}/")
    try:
        if int(getattr(advert, 'status', 0) or 0) != ADVERT_STATUS_PUBLISHED:
            return redirect(f"/adverts/{int(advert_id)}/")
    except Exception:
        return redirect(f"/adverts/{int(advert_id)}/")
    Advert.objects.filter(pk=int(advert_id)).update(created_at=timezone.now(), updated_at=timezone.now())
    return redirect('/adverts/')


def advert_delete(request: HttpRequest, advert_id: int) -> HttpResponse:
    if request.method != 'POST':
        return redirect(f"/adverts/{int(advert_id)}/")
    advert = get_object_or_404(Advert, pk=advert_id)
    if not _can_manage_advert(_get_admin_identity(request), advert):
        return redirect(f"/adverts/{int(advert_id)}/")
    _update_advert_status(int(advert_id), ADVERT_STATUS_DELETED)
    return redirect('/adverts/')
