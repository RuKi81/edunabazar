import json
import urllib.request
import urllib.parse
from functools import lru_cache
import random
import secrets
import datetime
import logging

from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.http import JsonResponse
from django.core.paginator import Paginator
from django.shortcuts import get_object_or_404, redirect, render
from django.db.models import Q, F, Prefetch
from django.contrib.auth.hashers import check_password, make_password
from django.utils import timezone
from django.contrib.gis.geos import Point
from django.core.mail import EmailMultiAlternatives
from django.core.validators import validate_email
from django.core.exceptions import ValidationError
from django.core import signing
from django.utils.http import url_has_allowed_host_and_scheme
from django.contrib.auth import authenticate, login as django_login
from PIL import Image as PILImage

from .models import Advert, LegacyUser, Catalog, Categories, AdvertPhoto, Seller, Review, Message


logger = logging.getLogger(__name__)


def _no_store(resp: HttpResponse) -> HttpResponse:
    try:
        resp['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        resp['Pragma'] = 'no-cache'
        resp['Expires'] = '0'
    except Exception:
        pass
    return resp



def _get_current_legacy_user(request: HttpRequest):
    _LOADED = '_cached_legacy_user_loaded'
    _ATTR = '_cached_legacy_user'
    if getattr(request, _LOADED, False):
        return getattr(request, _ATTR, None)
    legacy_user_id = request.session.get('legacy_user_id')
    user = LegacyUser.objects.filter(pk=legacy_user_id).first() if legacy_user_id else None
    setattr(request, _ATTR, user)
    setattr(request, _LOADED, True)
    return user


def _is_admin_user(user) -> bool:
    if not user:
        return False
    try:
        if bool(getattr(user, 'is_superuser', False)):
            return True
    except Exception:
        pass
    username = (getattr(user, 'username', '') or '').strip().lower()
    admin_usernames = getattr(settings, 'ADMIN_USERNAMES', {'admin'})
    return username in {u.lower() for u in admin_usernames}


def _get_admin_identity(request: HttpRequest):
    legacy_user = _get_current_legacy_user(request)
    if legacy_user is not None:
        return legacy_user
    try:
        django_user = getattr(request, 'user', None)
        if (
            django_user is not None
            and getattr(django_user, 'is_authenticated', False)
            and getattr(django_user, 'is_superuser', False)
        ):
            return django_user
    except Exception:
        return None


def _require_admin(request: HttpRequest):
    user = _get_admin_identity(request)
    if not user:
        return None, redirect(f"/login/?next={urllib.parse.quote(request.get_full_path())}")
    if not _is_admin_user(user):
        return None, redirect('/adverts/')
    return user, None


def _can_edit_advert(user, advert: Advert) -> bool:
    if not user:
        return False
    if _is_admin_user(user):
        return True
    try:
        return int(advert.author_id) == int(user.id) and int(advert.status) in (3, 5, 10)
    except Exception:
        return False


def _can_manage_advert(user, advert: Advert) -> bool:
    if not user:
        return False
    if _is_admin_user(user):
        return True
    try:
        return int(advert.author_id) == int(user.id) and int(advert.status) in (3, 10)
    except Exception:
        return False


_STATUS_LABELS = {10: 'Опубликовано', 5: 'На модерации', 3: 'Скрыто', 0: 'Удалено'}


def _annotate_adverts(adverts, user=None):
    for a in adverts:
        try:
            st = int(getattr(a, 'status', 0) or 0)
        except Exception:
            st = 0
        a.status_label = _STATUS_LABELS.get(st, f'Статус: {st}')
        if user is not None:
            a.can_edit = _can_edit_advert(user, a)
            a.can_manage = _can_manage_advert(user, a)
    return adverts


def _update_advert_status(advert_id: int, status: int) -> None:
    st = int(status)
    now = timezone.now()
    fields = {'status': st, 'updated_at': now}
    if st == 3:
        fields['hidden_at'] = now
        fields['deleted_at'] = None
    elif st == 0:
        fields['deleted_at'] = now
    elif st == 10:
        fields['hidden_at'] = None
        fields['deleted_at'] = None
    Advert.objects.filter(pk=int(advert_id)).update(**fields)


def _normalize_phone(phone: str) -> str:
    s = ''.join(ch for ch in (phone or '') if ch.isdigit() or ch == '+')
    s = s.strip()
    if not s:
        return ''
    if s.startswith('8') and len(s) == 11:
        s = '+7' + s[1:]
    if s.startswith('7') and len(s) == 11:
        s = '+7' + s[1:]
    if s.startswith('9') and len(s) == 10:
        s = '+7' + s
    if s.startswith('+'):
        return s
    return '+' + s


def home(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        'legacy/home.html',
        {
            'catalogs': Catalog.objects.filter(active=1).order_by('sort', 'title', 'id'),
            'categories': Categories.objects.filter(active=1).select_related('catalog').order_by('title'),
        },
    )


def advert_list(request: HttpRequest) -> HttpResponse:
    legacy_user = _get_current_legacy_user(request)
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
        qs = qs.exclude(status=0)
    else:
        qs = qs.filter(status=10)
    if q:
        qs = qs.filter(Q(title__icontains=q) | Q(text__icontains=q))

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

    catalog_id = None
    category_id = None
    try:
        catalog_id = int((request.GET.get('catalog') or '').strip() or 0) or None
    except Exception:
        catalog_id = None
    try:
        category_id = int((request.GET.get('category') or '').strip() or 0) or None
    except Exception:
        category_id = None
    if category_id is not None:
        qs = qs.filter(category_id=category_id)
    if catalog_id is not None:
        qs = qs.filter(category__catalog_id=catalog_id)

    if sort == 'price':
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
        },
    )
    return _no_store(resp)


def advert_detail(request: HttpRequest, advert_id: int) -> HttpResponse:
    advert = get_object_or_404(Advert.objects.select_related('category', 'author'), pk=advert_id)
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
                priority=0,
                created_at=now,
                updated_at=now,
                status=5,
            )
            for i, photo_file in enumerate(cleaned['photos']):
                AdvertPhoto.objects.create(advert=advert, image=photo_file, sort=i)
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
                AdvertPhoto.objects.create(advert=advert, image=photo_file, sort=next_sort + i)
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
    _update_advert_status(int(advert_id), 3)
    return redirect(f"/adverts/{int(advert_id)}/")


def advert_publish(request: HttpRequest, advert_id: int) -> HttpResponse:
    if request.method != 'POST':
        return redirect(f"/adverts/{int(advert_id)}/")
    advert = get_object_or_404(Advert.objects.select_related('author'), pk=advert_id)
    if not _can_manage_advert(_get_admin_identity(request), advert):
        return redirect(f"/adverts/{int(advert_id)}/")
    _update_advert_status(int(advert_id), 10)

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
        if int(getattr(advert, 'status', 0) or 0) != 10:
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
    _update_advert_status(int(advert_id), 0)
    return redirect('/adverts/')


def seller_list(request: HttpRequest) -> HttpResponse:
    q = (request.GET.get('q') or '').strip()
    qs = Seller.objects.select_related('user')
    if q:
        qs = qs.filter(Q(name__icontains=q) | Q(about__icontains=q) | Q(links__icontains=q))
    paginator = Paginator(qs.order_by('-created_at'), 12)
    page = paginator.get_page(request.GET.get('page') or 1)
    resp = render(
        request,
        'legacy/seller_list.html',
        {
            'sellers': page,
            'page_size': 12,
            'page_range': paginator.get_elided_page_range(page.number),
            'q': q,
            'legacy_user': _get_current_legacy_user(request),
        },
    )
    return _no_store(resp)


def seller_detail(request: HttpRequest, seller_id: int) -> HttpResponse:
    seller = get_object_or_404(Seller.objects.select_related('user'), pk=seller_id)
    try:
        contacts_display = json.dumps(seller.contacts, ensure_ascii=False, indent=2) if seller.contacts else ''
    except Exception:
        contacts_display = str(seller.contacts or '')

    legacy_user = _get_current_legacy_user(request)
    is_admin = _is_admin_user(legacy_user)
    reviews_qs = _get_reviews(Review.REVIEW_TYPE_SELLER, seller_id, include_moderation=is_admin)
    reviews_list = list(reviews_qs[:50])
    avg_rating = _avg_points(reviews_list)

    review_error = request.session.pop('review_error', '')
    review_success = request.session.pop('review_success', '')

    resp = render(
        request,
        'legacy/seller_detail.html',
        {
            'seller': seller,
            'contacts_display': contacts_display,
            'legacy_user': legacy_user,
            'reviews': reviews_list,
            'avg_rating': avg_rating,
            'review_error': review_error,
            'review_success': review_success,
            'review_type': Review.REVIEW_TYPE_SELLER,
            'review_object_id': seller_id,
            'is_admin_user': is_admin,
        },
    )
    return _no_store(resp)


def _parse_seller_form(post):
    name = (post.get('name') or '').strip()
    location = (post.get('location') or '').strip()
    contacts_raw = (post.get('contacts') or '').strip()
    links = (post.get('links') or '').strip()
    about = (post.get('about') or '').strip()

    errors = {}
    if not name:
        errors['name'] = 'Введите название'

    contacts = {}
    if contacts_raw:
        try:
            contacts = json.loads(contacts_raw)
            if not isinstance(contacts, dict):
                errors['contacts'] = 'Контакты должны быть JSON-объектом'
                contacts = {}
        except Exception:
            errors['contacts'] = 'Неверный формат JSON'

    initial = {'name': name, 'location': location, 'contacts': contacts_raw, 'links': links, 'about': about}
    cleaned = {'name': name, 'location': location, 'contacts': contacts, 'links': links, 'about': about}
    return cleaned, errors, initial


def seller_create(request: HttpRequest) -> HttpResponse:
    user = _get_current_legacy_user(request)
    if not user:
        return redirect(f"/login/?next={urllib.parse.quote(request.get_full_path())}")

    errors: dict = {}
    errors_all = ''
    initial: dict = {}

    if request.method == 'POST':
        cleaned, errors, initial = _parse_seller_form(request.POST)
        if not errors:
            if Seller.objects.filter(name=cleaned['name']).exists():
                errors['name'] = 'Предприятие с таким названием уже существует'
        if not errors:
            now = timezone.now()
            seller = Seller.objects.create(
                user_id=int(user.id),
                name=cleaned['name'],
                logo=0,
                location=cleaned['location'],
                contacts=cleaned['contacts'],
                price_list=0,
                links=cleaned['links'],
                about=cleaned['about'],
                created_at=now,
                updated_at=now,
                status=10,
            )
            return redirect(f"/sellers/{int(seller.id)}/")

    resp = render(
        request,
        'legacy/seller_form.html',
        {'mode': 'create', 'seller': None, 'initial': initial, 'errors': errors, 'errors_all': errors_all, 'legacy_user': user},
    )
    return _no_store(resp)


def seller_edit(request: HttpRequest, seller_id: int) -> HttpResponse:
    user = _get_current_legacy_user(request)
    if not user:
        return redirect(f"/login/?next={urllib.parse.quote(request.get_full_path())}")
    seller = get_object_or_404(Seller.objects.select_related('user'), pk=seller_id)
    if not _is_admin_user(user) and int(seller.user_id) != int(user.id):
        return redirect(f"/sellers/{int(seller_id)}/")

    errors: dict = {}
    errors_all = ''

    if request.method == 'POST':
        cleaned, errors, initial = _parse_seller_form(request.POST)
        if not errors:
            dup = Seller.objects.filter(name=cleaned['name']).exclude(pk=int(seller_id)).exists()
            if dup:
                errors['name'] = 'Предприятие с таким названием уже существует'
        if not errors:
            Seller.objects.filter(pk=int(seller_id)).update(
                name=cleaned['name'],
                location=cleaned['location'],
                contacts=cleaned['contacts'],
                links=cleaned['links'],
                about=cleaned['about'],
                updated_at=timezone.now(),
            )
            return redirect(f"/sellers/{int(seller_id)}/")
    else:
        contacts_str = ''
        try:
            contacts_str = json.dumps(seller.contacts, ensure_ascii=False, indent=2) if seller.contacts else ''
        except Exception:
            contacts_str = ''
        initial = {
            'name': seller.name or '',
            'location': seller.location or '',
            'contacts': contacts_str,
            'links': seller.links or '',
            'about': seller.about or '',
        }

    resp = render(
        request,
        'legacy/seller_form.html',
        {'mode': 'edit', 'seller': seller, 'initial': initial, 'errors': errors, 'errors_all': errors_all, 'legacy_user': user},
    )
    return _no_store(resp)


def map_view(request: HttpRequest) -> HttpResponse:
    resp = render(
        request,
        'legacy/map.html',
        {
            'catalogs': Catalog.objects.filter(active=1).order_by('sort', 'title', 'id'),
            'categories': Categories.objects.filter(active=1).select_related('catalog').order_by('title'),
        },
    )
    return _no_store(resp)


def map_adverts_api(request: HttpRequest) -> JsonResponse:
    limit_raw = (request.GET.get('limit') or '500').strip()
    try:
        limit = int(limit_raw)
    except Exception:
        limit = 500
    limit = max(1, min(limit, 2000))

    q = (request.GET.get('q') or '').strip()
    type_raw = (request.GET.get('type') or '').strip().lower()
    opt_raw = (request.GET.get('opt') or '').strip()
    delivery_raw = (request.GET.get('delivery') or '').strip()
    catalog_raw = (request.GET.get('catalog') or '').strip()
    category_raw = (request.GET.get('category') or '').strip()
    sort_raw = (request.GET.get('sort') or '').strip().lower()

    user = _get_current_legacy_user(request)
    is_admin = _is_admin_user(user)

    _photos_prefetch = Prefetch(
        'photos',
        queryset=AdvertPhoto.objects.order_by('sort', 'id'),
        to_attr='prefetched_photos',
    )
    qs = Advert.objects.select_related('category').prefetch_related(_photos_prefetch).exclude(status=0)
    if not is_admin:
        qs = qs.filter(status=10)

    if q:
        qs = qs.filter(Q(title__icontains=q) | Q(text__icontains=q))

    if type_raw in {'offer', 'demand'}:
        qs = qs.filter(type=0 if type_raw == 'offer' else 1)

    if opt_raw and opt_raw.strip().lower() in {'1', 'true', 'yes', 'on'}:
        qs = qs.filter(wholesale_price__gt=0)

    if delivery_raw and delivery_raw.strip().lower() in {'1', 'true', 'yes', 'on'}:
        qs = qs.filter(delivery=True)

    try:
        if category_raw:
            qs = qs.filter(category_id=int(category_raw))
    except Exception:
        pass

    try:
        if catalog_raw:
            qs = qs.filter(category__catalog_id=int(catalog_raw))
    except Exception:
        pass

    if sort_raw == 'price':
        qs = qs.order_by('price', '-created_at', '-id')
    else:
        qs = qs.order_by('-created_at', '-id')

    qs = qs[:limit]

    adverts = []
    for a in qs:
        try:
            loc = getattr(a, 'location', None)
            lat = float(loc.y) if loc else None
            lon = float(loc.x) if loc else None
        except Exception:
            lat = None
            lon = None

        text = (getattr(a, 'text', '') or '').strip()
        text_short = text
        if len(text_short) > 160:
            text_short = text_short[:160].rstrip() + '…'

        category_title = ''
        try:
            cat = getattr(a, 'category', None)
            category_title = (getattr(cat, 'title', '') or '').strip()
        except Exception:
            category_title = ''

        thumb_url = ''
        try:
            prefetched = getattr(a, 'prefetched_photos', None)
            photo = prefetched[0] if prefetched else None
            if photo and getattr(photo, 'image', None):
                thumb_url = getattr(photo.image, 'url', '') or ''
        except Exception:
            thumb_url = ''

        adverts.append(
            {
                'id': int(a.id),
                'title': getattr(a, 'title', '') or '',
                'lat': lat,
                'lon': lon,
                'category_id': int(a.category_id) if a.category_id is not None else None,
                'category_title': category_title,
                'price': getattr(a, 'price', None),
                'text_short': text_short,
                'url': f"/adverts/{int(a.id)}/",
                'thumb_url': thumb_url,
                'created_date': timezone.localtime(getattr(a, 'created_at', timezone.now())).strftime('%d.%m.%Y'),
                'is_opt': bool((getattr(a, 'wholesale_price', 0) or 0) > 0),
                'is_delivery': bool(getattr(a, 'delivery', False)),
            }
        )

    return JsonResponse({'ok': True, 'adverts': adverts})


def map_categories_api(request: HttpRequest) -> JsonResponse:
    limit_raw = (request.GET.get('limit') or '500').strip()
    try:
        limit = int(limit_raw)
    except Exception:
        limit = 500
    limit = max(1, min(limit, 2000))
    qs = Categories.objects.filter(active=1).order_by('title')[:limit]
    return JsonResponse({'ok': True, 'items': [{'id': int(c.id), 'title': c.title} for c in qs]})


@lru_cache(maxsize=256)
def _geocode_ru(q: str):
    q = (q or '').strip()
    if not q:
        return None
    try:
        url = 'https://nominatim.openstreetmap.org/search?' + urllib.parse.urlencode(
            {'format': 'json', 'q': q, 'limit': 1}
        )
        req = urllib.request.Request(url, headers={'User-Agent': 'enb-legacy/1.0'})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode('utf-8') or '[]')
        if not data:
            return None
        row = data[0]
        return float(row.get('lat')), float(row.get('lon')), (row.get('display_name') or '')
    except Exception:
        return None


def geocode_api(request: HttpRequest) -> JsonResponse:
    q = (request.GET.get('q') or '').strip()
    g = _geocode_ru(q)
    if not g:
        return JsonResponse({'ok': True, 'found': False})
    lat, lon, display_name = g
    return JsonResponse({'ok': True, 'found': True, 'lat': lat, 'lon': lon, 'display_name': display_name})


def reverse_geocode_api(request: HttpRequest) -> JsonResponse:
    lat_raw = (request.GET.get('lat') or '').strip().replace(',', '.')
    lon_raw = (request.GET.get('lon') or '').strip().replace(',', '.')
    try:
        lat = float(lat_raw)
        lon = float(lon_raw)
    except Exception:
        return JsonResponse({'ok': False})
    try:
        url = 'https://nominatim.openstreetmap.org/reverse?' + urllib.parse.urlencode(
            {'format': 'json', 'lat': lat, 'lon': lon, 'zoom': 18}
        )
        req = urllib.request.Request(url, headers={'User-Agent': 'enb-legacy/1.0'})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode('utf-8') or '{}')
        return JsonResponse({'ok': True, 'address': (data.get('display_name') or '')})
    except Exception:
        return JsonResponse({'ok': False})


_LOGIN_MAX_FAILS = 10
_LOGIN_LOCKOUT_SECONDS = 15 * 60


def legacy_login(request: HttpRequest) -> HttpResponse:
    next_param = (request.POST.get('next') or request.GET.get('next') or '').strip()

    fails_key = 'login_fail_count'
    locked_until_key = 'login_locked_until'
    now_ts = timezone.now().timestamp()

    locked_until = request.session.get(locked_until_key) or 0
    if now_ts < locked_until:
        remaining = int(locked_until - now_ts)
        return render(request, 'legacy/login.html', {
            'error': f'Слишком много попыток. Попробуйте через {remaining // 60 + 1} мин.',
            'next': next_param,
        })

    if request.method == 'POST':
        username = (request.POST.get('username') or '').strip()
        password = (request.POST.get('password') or '').strip()
        if username and password:
            user = LegacyUser.objects.filter(username=username).first()
            if user and check_password(password, user.password_hash or ''):
                request.session.pop(fails_key, None)
                request.session.pop(locked_until_key, None)
                request.session['legacy_user_id'] = int(user.id)
                django_user = authenticate(request, username=username, password=password)
                if django_user is not None:
                    django_login(request, django_user)
                if next_param and url_has_allowed_host_and_scheme(
                    url=next_param,
                    allowed_hosts={request.get_host()},
                    require_https=request.is_secure(),
                ):
                    return redirect(next_param)
                return redirect('/adverts/')

        fail_count = int(request.session.get(fails_key) or 0) + 1
        request.session[fails_key] = fail_count
        if fail_count >= _LOGIN_MAX_FAILS:
            request.session[locked_until_key] = now_ts + _LOGIN_LOCKOUT_SECONDS
            request.session[fails_key] = 0
            return render(request, 'legacy/login.html', {
                'error': f'Слишком много попыток. Попробуйте через {_LOGIN_LOCKOUT_SECONDS // 60} мин.',
                'next': next_param,
            })
        return render(
            request,
            'legacy/login.html',
            {
                'error': 'Неверный логин или пароль',
                'username': username,
                'next': next_param,
            },
        )
    return render(request, 'legacy/login.html', {'next': next_param})


def legacy_logout(request: HttpRequest) -> HttpResponse:
    request.session.flush()
    return redirect('/adverts/')


def legacy_register_start(request: HttpRequest) -> HttpResponse:
    resp = render(
        request,
        'legacy/register_start.html',
        {
            'legacy_user': _get_current_legacy_user(request),
        },
    )
    return _no_store(resp)


def legacy_register_sms(request: HttpRequest) -> HttpResponse:
    if request.method == 'POST':
        phone_raw = (request.POST.get('phone') or '').strip()
        phone = _normalize_phone(phone_raw)
        errors: dict[str, str] = {}
        if not phone or len(phone) < 10:
            errors['phone'] = 'Введите корректный телефон'
        elif LegacyUser.objects.filter(phone=phone).exists():
            errors['phone'] = 'Этот телефон уже зарегистрирован'
        if errors:
            resp = render(
                request,
                'legacy/register_sms.html',
                {
                    'errors': errors,
                    'form': {'phone': phone_raw},
                    'legacy_user': _get_current_legacy_user(request),
                },
            )
            return _no_store(resp)

        otp_code = f"{random.randint(0, 999999):06d}"
        request.session['sms_register'] = {
            'phone': phone,
            'code': otp_code,
            'created_at': timezone.now().isoformat(),
            'attempts': 0,
            'verify_attempts': 0,
        }
        request.session.modified = True
        return redirect('/register/sms/confirm/')

    resp = render(
        request,
        'legacy/register_sms.html',
        {
            'errors': {},
            'form': {'phone': ''},
            'legacy_user': _get_current_legacy_user(request),
        },
    )
    return _no_store(resp)


def admin_users(request: HttpRequest) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny
    q = (request.GET.get('q') or '').strip()
    qs = LegacyUser.objects.all()
    if q:
        qs = qs.filter(Q(username__icontains=q) | Q(email__icontains=q) | Q(name__icontains=q))
    paginator = Paginator(qs.order_by('-created_at', '-id'), 25)
    page = paginator.get_page(request.GET.get('page') or 1)
    page_range = paginator.get_elided_page_range(page.number)
    resp = render(
        request,
        'legacy/admin_users.html',
        {
            'legacy_user': admin_user,
            'users': page,
            'page_size': 25,
            'page_range': page_range,
            'q': q,
        },
    )
    return _no_store(resp)


def admin_users_bulk_delete(request: HttpRequest) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny
    if request.method != 'POST':
        return redirect('/legacy-admin/')
    raw_ids = request.POST.getlist('user_id')
    next_raw = (request.POST.get('next') or '').strip()
    safe_next = '/legacy-admin/'
    if next_raw and url_has_allowed_host_and_scheme(
        url=next_raw,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        safe_next = next_raw

    ids = []
    for rid in raw_ids:
        try:
            ids.append(int(str(rid).strip()))
        except Exception:
            continue
    ids = sorted({x for x in ids if x > 0})
    if not ids:
        return redirect(safe_next)

    protected_ids = set(LegacyUser.objects.filter(username__iexact='admin').values_list('id', flat=True))
    delete_ids = [uid for uid in ids if uid not in protected_ids]
    if not delete_ids:
        return redirect(safe_next)

    Advert.objects.filter(author_id__in=delete_ids).update(status=0, deleted_at=timezone.now(), updated_at=timezone.now())
    LegacyUser.objects.filter(id__in=delete_ids).delete()
    return redirect(safe_next)


def admin_catalogs(request: HttpRequest) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny
    ok_message = ''
    if request.method == 'POST':
        catalogs = Catalog.objects.all().order_by('sort', 'title', 'id')
        for c in catalogs:
            raw = (request.POST.get(f"sort_{int(c.id)}") or '').strip()
            try:
                sort_val = int(raw) if raw else 0
            except Exception:
                sort_val = 0
            active_val = 1 if (request.POST.get(f"active_{int(c.id)}") or '').strip().lower() in {'1', 'true', 'on', 'yes'} else 0
            Catalog.objects.filter(pk=int(c.id)).update(sort=sort_val, active=active_val)
        ok_message = 'Сохранено'
    catalogs = Catalog.objects.all().order_by('sort', 'title', 'id')
    resp = render(
        request,
        'legacy/admin_catalogs.html',
        {'legacy_user': admin_user, 'catalogs': catalogs, 'ok_message': ok_message},
    )
    return _no_store(resp)


def admin_user_detail(request: HttpRequest, user_id: int) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny
    u = get_object_or_404(LegacyUser, pk=user_id)
    errors: dict[str, str] = {}
    ok_message = ''
    if request.method == 'POST':
        new_password = (request.POST.get('new_password') or '').strip()
        if not new_password:
            errors['new_password'] = 'Введите новый пароль'
        elif len(new_password) < 4:
            errors['new_password'] = 'Пароль слишком короткий'
        if not errors:
            LegacyUser.objects.filter(pk=int(u.id)).update(password_hash=make_password(new_password), updated_at=timezone.now())
            ok_message = 'Пароль изменён'
    adverts = Advert.objects.filter(author_id=u.id).select_related('category').order_by('-created_at', '-id')
    resp = render(
        request,
        'legacy/admin_user_detail.html',
        {'legacy_user': admin_user, 'u': u, 'errors': errors, 'ok_message': ok_message, 'adverts': adverts},
    )
    return _no_store(resp)


def change_password(request: HttpRequest) -> HttpResponse:
    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/?next=/change-password/')

    errors: dict[str, str] = {}
    saved = False

    if request.method == 'POST':
        old_pw = (request.POST.get('old_password') or '').strip()
        new_pw1 = (request.POST.get('new_password1') or '').strip()
        new_pw2 = (request.POST.get('new_password2') or '').strip()

        if not old_pw:
            errors['old_password'] = 'Введите текущий пароль'
        elif not check_password(old_pw, user.password_hash or ''):
            errors['old_password'] = 'Неверный пароль'

        if not new_pw1:
            errors['new_password1'] = 'Введите новый пароль'
        elif len(new_pw1) < 6:
            errors['new_password1'] = 'Пароль слишком короткий (минимум 6 символов)'

        if new_pw1 and new_pw2 and new_pw1 != new_pw2:
            errors['new_password2'] = 'Пароли не совпадают'

        if not errors:
            new_auth_key = secrets.token_hex(16)[:32]
            LegacyUser.objects.filter(pk=int(user.id)).update(
                password_hash=make_password(new_pw1),
                auth_key=new_auth_key,
                updated_at=timezone.now(),
            )
            saved = True

    resp = render(request, 'legacy/change_password.html', {
        'legacy_user': user,
        'errors': errors,
        'saved': saved,
    })
    return _no_store(resp)


def _send_email(to_email: str, subject: str, body: str) -> bool:
    to_email = (to_email or '').strip()
    if not to_email:
        return False
    try:
        validate_email(to_email)
    except ValidationError:
        logger.warning('Invalid email address: %s', to_email)
        return False

    from_email = (getattr(settings, 'DEFAULT_FROM_EMAIL', '') or '').strip() or None
    try:
        msg = EmailMultiAlternatives(
            subject=str(subject or '').strip(),
            body=str(body or ''),
            from_email=from_email,
            to=[to_email],
        )
        msg.send(fail_silently=False)
        return True
    except Exception:
        logger.exception('Failed to send email to=%s subject=%s', to_email, subject)
        return False


def _make_set_password_token(user_id: int, auth_key: str) -> str:
    payload = {
        'uid': int(user_id),
        'ak': str(auth_key or ''),
    }
    return signing.dumps(payload, salt='legacy-set-password')


def _send_registration_email(user_email: str, username: str, set_password_url: str) -> bool:
    subject = 'Регистрация на сайте'
    body = (
        'Вы зарегистрировали профиль.\n\n'
        f'Логин: {username}\n\n'
        'Установите пароль по ссылке (она одноразовая и скоро истечёт):\n'
        f'{set_password_url}\n'
    )
    return _send_email(user_email, subject, body)


def _send_advert_published_email(user_email: str, advert_title: str, advert_url: str) -> bool:
    subject = 'Ваше объявление опубликовано'
    body = (
        'Ваше объявление прошло модерацию и опубликовано.\n\n'
        f'Объявление: {advert_title}\n'
        f'Ссылка: {advert_url}\n'
    )
    return _send_email(user_email, subject, body)


def legacy_set_password(request: HttpRequest, token: str) -> HttpResponse:
    errors: dict[str, str] = {}
    token = (token or '').strip()
    if not token:
        return render(request, 'legacy/set_password.html', {'errors': {'token': 'Неверная ссылка'}})

    next_raw = (request.GET.get('next') or '').strip()
    if request.method == 'POST':
        next_raw = (request.POST.get('next') or next_raw or '').strip()
    next_url = ''
    if next_raw and url_has_allowed_host_and_scheme(
        url=next_raw,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        next_url = next_raw

    try:
        payload = signing.loads(token, salt='legacy-set-password', max_age=60 * 60 * 24)
        user_id = int(payload.get('uid') or 0)
        auth_key = str(payload.get('ak') or '')
    except Exception:
        user_id = 0
        auth_key = ''

    user = None
    if user_id > 0 and auth_key:
        user = LegacyUser.objects.filter(pk=user_id, auth_key=auth_key).first()
    if user is None:
        return render(request, 'legacy/set_password.html', {'errors': {'token': 'Ссылка недействительна или устарела'}})

    if request.method == 'POST':
        p1 = (request.POST.get('password1') or '').strip()
        p2 = (request.POST.get('password2') or '').strip()
        if not p1:
            errors['password1'] = 'Введите пароль'
        elif len(p1) < 6:
            errors['password1'] = 'Пароль слишком короткий'
        if p1 and p2 and p1 != p2:
            errors['password2'] = 'Пароли не совпадают'

        if not errors:
            new_auth_key = secrets.token_hex(16)[:32]
            try:
                updated = LegacyUser.objects.filter(pk=int(user.id), auth_key=auth_key).update(
                    password_hash=make_password(p1),
                    auth_key=new_auth_key,
                    updated_at=timezone.now(),
                )
            except Exception:
                updated = 0
            if not updated:
                return render(
                    request,
                    'legacy/set_password.html',
                    {'errors': {'token': 'Ссылка уже использована или устарела'}},
                )

            request.session['legacy_user_id'] = int(user.id)
            return redirect(next_url or '/adverts/')

    return render(
        request,
        'legacy/set_password.html',
        {
            'errors': errors,
            'token': token,
            'next': next_url,
            'legacy_user': _get_current_legacy_user(request),
        },
    )


def legacy_register_email(request: HttpRequest) -> HttpResponse:
    if request.method == 'POST':
        username = (request.POST.get('username') or '').strip()
        email = (request.POST.get('email') or '').strip()
        name = (request.POST.get('name') or '').strip()
        phone = (request.POST.get('phone') or '').strip()

        errors = {}
        if not username:
            errors['username'] = 'Введите username'
        if not email:
            errors['email'] = 'Введите email'

        if username and LegacyUser.objects.filter(username=username).exists():
            errors['username'] = 'Этот username уже занят'
        if email and LegacyUser.objects.filter(email=email).exists():
            errors['email'] = 'Этот email уже занят'

        if errors:
            resp = render(
                request,
                'legacy/register.html',
                {
                    'errors': errors,
                    'form': {
                        'username': username,
                        'email': email,
                        'name': name,
                        'phone': phone,
                    },
                    'legacy_user': _get_current_legacy_user(request),
                },
            )
            return _no_store(resp)

        pw_hash = make_password(secrets.token_urlsafe(12))
        auth_key = secrets.token_hex(16)[:32]
        now = timezone.now()

        new_user = LegacyUser.objects.create(
            type=0,
            username=username,
            auth_key=auth_key,
            password_hash=pw_hash,
            email=email,
            currency='RU',
            name=name,
            address='',
            phone=phone,
            inn='',
            status=10,
            created_at=now,
            updated_at=now,
            contacts='',
        )

        request.session['legacy_user_id'] = int(new_user.id)

        next_raw = (request.POST.get('next') or request.GET.get('next') or '').strip()
        safe_next = ''
        if next_raw and url_has_allowed_host_and_scheme(
            url=next_raw,
            allowed_hosts={request.get_host()},
            require_https=request.is_secure(),
        ):
            safe_next = next_raw

        try:
            token = _make_set_password_token(int(new_user.id), auth_key)
            set_pw_url = request.build_absolute_uri(f"/set-password/{token}/")
            if safe_next:
                set_pw_url = f"{set_pw_url}?next={urllib.parse.quote(safe_next)}"
            _send_registration_email(email, username, set_pw_url)
        except Exception:
            logger.exception('Failed to dispatch registration email (email flow)')

        return redirect(safe_next or '/adverts/')

    resp = render(
        request,
        'legacy/register.html',
        {
            'errors': {},
            'form': {
                'username': '',
                'email': '',
                'name': '',
                'phone': '',
            },
            'legacy_user': _get_current_legacy_user(request),
        },
    )
    return _no_store(resp)


def legacy_register_sms_confirm(request: HttpRequest) -> HttpResponse:
    state = request.session.get('sms_register') or {}
    phone = (state.get('phone') or '').strip()
    otp_code = (state.get('code') or '').strip()
    created_at_raw = (state.get('created_at') or '').strip()
    verify_attempts = int(state.get('verify_attempts') or 0)

    errors: dict[str, str] = {}
    errors_all: str = ''
    now = timezone.now()

    created_at = None
    if created_at_raw:
        try:
            created_at = datetime.datetime.fromisoformat(created_at_raw)
            if timezone.is_naive(created_at):
                created_at = timezone.make_aware(created_at)
        except Exception:
            created_at = None

    if not phone or not otp_code or not created_at:
        errors_all = 'Сначала запросите SMS-код'
    else:
        ttl_seconds = 5 * 60
        if (now - created_at).total_seconds() > ttl_seconds:
            errors_all = 'Код устарел. Запросите новый'

    if request.method == 'POST' and not errors_all:
        code_entered = (request.POST.get('code') or '').strip()
        username = (request.POST.get('username') or '').strip()
        email = (request.POST.get('email') or '').strip()
        name = (request.POST.get('name') or '').strip()
        address = (request.POST.get('address') or '').strip()
        show_address_raw = (request.POST.get('show_address') or '').strip().lower()
        lat_raw = (request.POST.get('lat') or '').strip().replace(',', '.')
        lon_raw = (request.POST.get('lon') or '').strip().replace(',', '.')

        show_address = 1 if show_address_raw in {'1', 'true', 'yes', 'on'} else 0

        lat = None
        lon = None
        try:
            if lat_raw and lon_raw:
                lat = float(lat_raw)
                lon = float(lon_raw)
        except (TypeError, ValueError):
            lat = None
            lon = None

        if lat is not None and not (-90 <= lat <= 90):
            lat = None
            lon = None
        if lon is not None and not (-180 <= lon <= 180):
            lat = None
            lon = None

        if not code_entered:
            errors['code'] = 'Введите код'
        if not username:
            errors['username'] = 'Введите username'
        if not email:
            errors['email'] = 'Введите email'

        if username and LegacyUser.objects.filter(username=username).exists():
            errors['username'] = 'Этот username уже занят'
        if email and LegacyUser.objects.filter(email=email).exists():
            errors['email'] = 'Этот email уже занят'
        if phone and LegacyUser.objects.filter(phone=phone).exists():
            errors['phone'] = 'Этот телефон уже зарегистрирован'

        if not errors:
            if verify_attempts >= 10:
                errors_all = 'Слишком много попыток. Запросите новый код'
            elif code_entered != otp_code:
                verify_attempts += 1
                state['verify_attempts'] = verify_attempts
                request.session['sms_register'] = state
                request.session.modified = True
                errors['code'] = 'Неверный код'

        if not errors:
            pw_hash = make_password(secrets.token_urlsafe(12))
            auth_key = secrets.token_hex(16)[:32]

            now = timezone.now()
            contacts_val = 'show_address=0' if not show_address else ''
            create_kwargs = dict(
                type=0,
                username=username,
                auth_key=auth_key,
                password_hash=pw_hash,
                email=email,
                currency='RU',
                name=name,
                address=address,
                phone=phone,
                inn='',
                status=10,
                created_at=now,
                updated_at=now,
                contacts=contacts_val,
            )
            if lat is not None and lon is not None:
                create_kwargs['location'] = Point(float(lon), float(lat), srid=4326)

            new_user = LegacyUser.objects.create(**create_kwargs)

            request.session.pop('sms_register', None)
            request.session['legacy_user_id'] = int(new_user.id)

            next_raw = (request.POST.get('next') or request.GET.get('next') or '').strip()
            safe_next = ''
            if next_raw and url_has_allowed_host_and_scheme(
                url=next_raw,
                allowed_hosts={request.get_host()},
                require_https=request.is_secure(),
            ):
                safe_next = next_raw

            try:
                token = _make_set_password_token(int(new_user.id), auth_key)
                set_pw_url = request.build_absolute_uri(f"/set-password/{token}/")
                if safe_next:
                    set_pw_url = f"{set_pw_url}?next={urllib.parse.quote(safe_next)}"
                _send_registration_email(email, username, set_pw_url)
            except Exception:
                logger.exception('Failed to dispatch registration email (sms flow)')
            return redirect(safe_next or '/adverts/')

    resp = render(
        request,
        'legacy/register_sms_confirm.html',
        {
            'errors': errors,
            'errors_all': errors_all,
            'legacy_user': _get_current_legacy_user(request),
            'form': {
                'username': request.POST.get('username', '') if request.method == 'POST' else '',
                'email': request.POST.get('email', '') if request.method == 'POST' else '',
                'name': request.POST.get('name', '') if request.method == 'POST' else '',
                'code': request.POST.get('code', '') if request.method == 'POST' else '',
                'address': request.POST.get('address', '') if request.method == 'POST' else '',
                'lat': request.POST.get('lat', '') if request.method == 'POST' else '',
                'lon': request.POST.get('lon', '') if request.method == 'POST' else '',
                'show_address': (request.POST.get('show_address', '') if request.method == 'POST' else ''),
            },
            'phone': phone,
            'dev_code': otp_code if settings.DEBUG else '',
        },
    )
    return _no_store(resp)


def legacy_me(request: HttpRequest) -> HttpResponse:
    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/?next=/me/')

    errors = {}
    saved = False

    is_admin = _is_admin_user(user)

    my_qs = Advert.objects.filter(author_id=user.id).exclude(status=0)

    my_adverts = _annotate_adverts(
        list(my_qs.select_related('category').order_by('-created_at', '-id')),
        user=user,
    )

    admin_status = (request.GET.get('status') or 'all').strip().lower()
    admin_sort = (request.GET.get('sort') or 'created').strip().lower()
    if admin_status not in {'all', 'published', 'moderation', 'hidden'}:
        admin_status = 'all'
    if admin_sort not in {'created', 'hidden'}:
        admin_sort = 'created'

    admin_adverts = []
    if is_admin:
        qs = Advert.objects.select_related('category', 'author').exclude(status=0)
        if admin_status == 'published':
            qs = qs.filter(status=10)
        elif admin_status == 'moderation':
            qs = qs.filter(status=5)
        elif admin_status == 'hidden':
            qs = qs.filter(status=3)

        if admin_sort == 'hidden':
            qs = qs.order_by(F('hidden_at').desc(nulls_last=True), '-id')
        else:
            qs = qs.order_by('-created_at', '-id')

        admin_adverts = _annotate_adverts(list(qs[:500]))
        for a in admin_adverts:
            try:
                a.author_label = str(getattr(getattr(a, 'author', None), 'username', '') or '')
            except Exception:
                a.author_label = ''

    current_contacts = str(getattr(user, 'contacts', '') or '')
    show_address_enabled = ('show_address=0' not in current_contacts)

    if request.method == 'POST':
        email = (request.POST.get('email') or '').strip()
        name = (request.POST.get('name') or '').strip()
        phone = (request.POST.get('phone') or '').strip()
        address = (request.POST.get('address') or '').strip()
        show_address_raw = (request.POST.get('show_address') or '').strip().lower()
        show_address_new = show_address_raw in {'1', 'true', 'yes', 'on'}

        lat_raw = (request.POST.get('lat') or '').strip().replace(',', '.')
        lon_raw = (request.POST.get('lon') or '').strip().replace(',', '.')
        lat = None
        lon = None
        try:
            if lat_raw and lon_raw:
                lat = float(lat_raw)
                lon = float(lon_raw)
        except (TypeError, ValueError):
            lat = None
            lon = None

        if lat is not None and not (-90 <= lat <= 90):
            lat = None
            lon = None
        if lon is not None and not (-180 <= lon <= 180):
            lat = None
            lon = None

        if not email:
            errors['email'] = 'Введите email'
        elif LegacyUser.objects.filter(email=email).exclude(pk=user.pk).exists():
            errors['email'] = 'Этот email уже занят'

        if not errors:
            contacts_new = str(getattr(user, 'contacts', '') or '')
            contacts_new = contacts_new.replace('show_address=1', '')
            if show_address_new:
                contacts_new = contacts_new.replace('show_address=0', '')
            else:
                if 'show_address=0' not in contacts_new:
                    contacts_new = (contacts_new + ('; ' if contacts_new.strip() else '') + 'show_address=0').strip()

            contacts_new = contacts_new.replace(';;', ';')
            contacts_new = contacts_new.strip(' ;\n\t')

            update_fields = ['email', 'name', 'phone', 'address', 'contacts', 'updated_at']
            user.email = email
            user.name = name
            user.phone = phone
            user.address = address
            user.contacts = contacts_new
            user.updated_at = timezone.now()
            if lat is not None and lon is not None:
                user.location = Point(float(lon), float(lat), srid=4326)
                update_fields.append('location')
            user.save(update_fields=update_fields)
            saved = True

            show_address_enabled = show_address_new

    return render(
        request,
        'legacy/me.html',
        {
            'legacy_user': user,
            'errors': errors,
            'saved': saved,
            'show_address_enabled': show_address_enabled,
            'my_adverts': my_adverts,
            'is_admin_cabinet': is_admin,
            'admin_adverts': admin_adverts,
            'admin_filter_status': admin_status,
            'admin_filter_sort': admin_sort,
        },
    )


def legacy_me_bulk_adverts(request: HttpRequest) -> HttpResponse:
    if request.method != 'POST':
        return redirect('/me/')

    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/?next=/me/')

    action = (request.POST.get('action') or '').strip().lower()
    ids_raw = request.POST.getlist('advert_id')

    ids: list[int] = []
    for x in ids_raw:
        try:
            v = int(str(x).strip())
        except Exception:
            continue
        if v > 0:
            ids.append(v)
    ids = sorted(set(ids))

    if not ids:
        return redirect('/me/')

    is_admin = _is_admin_user(user)
    allow_publish = is_admin

    if action not in {'hide', 'delete', 'bump', 'publish'}:
        return redirect('/me/')
    if action == 'publish' and not allow_publish:
        return redirect('/me/')

    qs = Advert.objects.filter(pk__in=ids).only('id', 'author_id', 'status')
    advert_meta = {a.id: a for a in qs}

    def can_touch(advert_obj) -> bool:
        if is_admin:
            return True
        return int(getattr(advert_obj, 'author_id', 0) or 0) == int(getattr(user, 'id', 0) or 0)

    now = timezone.now()
    for advert_id in ids:
        obj = advert_meta.get(advert_id)
        if obj is None or not can_touch(obj):
            continue
        if action == 'hide':
            _update_advert_status(advert_id, 3)
        elif action == 'delete':
            _update_advert_status(advert_id, 0)
        elif action == 'publish':
            _update_advert_status(advert_id, 10)
        elif action == 'bump':
            if int(getattr(obj, 'status', 0) or 0) == 10:
                Advert.objects.filter(pk=advert_id).update(created_at=now, updated_at=now)

    next_raw = (request.POST.get('next') or '').strip()
    if next_raw and url_has_allowed_host_and_scheme(
        url=next_raw,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        return redirect(next_raw)
    return redirect('/me/')


# ---------------------------------------------------------------------------
# Reviews
# ---------------------------------------------------------------------------

_REVIEW_STATUS_PUBLISHED = 10
_REVIEW_STATUS_MODERATION = 5
_REVIEW_STATUS_HIDDEN = 3
_REVIEW_STATUS_DELETED = 0


def _get_reviews(review_type: int, object_id: int, include_moderation: bool = False):
    """Return published reviews for an object. If include_moderation, include status=5 too."""
    qs = Review.objects.select_related('author').filter(type=review_type, object_id=object_id)
    if include_moderation:
        qs = qs.filter(status__in=[_REVIEW_STATUS_PUBLISHED, _REVIEW_STATUS_MODERATION])
    else:
        qs = qs.filter(status=_REVIEW_STATUS_PUBLISHED)
    return qs.order_by('-created_at', '-id')


def _avg_points(reviews) -> float | None:
    """Calculate average points from a queryset/list of reviews."""
    total = 0
    count = 0
    for r in reviews:
        try:
            total += int(r.points)
            count += 1
        except Exception:
            pass
    return round(total / count, 1) if count else None


def review_create(request: HttpRequest) -> HttpResponse:
    """Create a review for an advert (type=0) or seller (type=1)."""
    if request.method != 'POST':
        return redirect('/adverts/')

    user = _get_current_legacy_user(request)
    if not user:
        return redirect(f"/login/?next={urllib.parse.quote(request.META.get('HTTP_REFERER', '/adverts/'))}")

    review_type_raw = (request.POST.get('review_type') or '').strip()
    object_id_raw = (request.POST.get('object_id') or '').strip()
    points_raw = (request.POST.get('points') or '').strip()
    text = (request.POST.get('text') or '').strip()

    try:
        review_type = int(review_type_raw)
        if review_type not in {Review.REVIEW_TYPE_ADVERT, Review.REVIEW_TYPE_SELLER}:
            review_type = Review.REVIEW_TYPE_ADVERT
    except Exception:
        review_type = Review.REVIEW_TYPE_ADVERT

    try:
        object_id = int(object_id_raw)
    except Exception:
        return redirect('/adverts/')

    try:
        points = int(points_raw)
        points = max(1, min(5, points))
    except Exception:
        points = 5

    if not text:
        request.session['review_error'] = 'Введите текст отзыва'
        if review_type == Review.REVIEW_TYPE_SELLER:
            return redirect(f"/sellers/{object_id}/")
        return redirect(f"/adverts/{object_id}/")

    if len(text) > 2000:
        text = text[:2000]

    # Prevent duplicate: one review per user per object
    existing = Review.objects.filter(
        type=review_type, object_id=object_id, author_id=user.id,
    ).exclude(status=_REVIEW_STATUS_DELETED).exists()
    if existing:
        request.session['review_error'] = 'Вы уже оставили отзыв'
        if review_type == Review.REVIEW_TYPE_SELLER:
            return redirect(f"/sellers/{object_id}/")
        return redirect(f"/adverts/{object_id}/")

    now = timezone.now()
    Review.objects.create(
        type=review_type,
        object_id=object_id,
        points=points,
        author_id=int(user.id),
        text=text,
        created_at=now,
        updated_at=now,
        status=_REVIEW_STATUS_MODERATION,
    )

    request.session['review_success'] = 'Отзыв отправлен на модерацию'
    if review_type == Review.REVIEW_TYPE_SELLER:
        return redirect(f"/sellers/{object_id}/")
    return redirect(f"/adverts/{object_id}/")


def review_delete(request: HttpRequest, review_id: int) -> HttpResponse:
    """Delete (soft) a review. Author or admin only."""
    if request.method != 'POST':
        return redirect('/adverts/')

    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/')

    review = get_object_or_404(Review, pk=review_id)
    is_author = int(review.author_id) == int(user.id)
    is_admin = _is_admin_user(user)
    if not is_author and not is_admin:
        return redirect('/adverts/')

    Review.objects.filter(pk=review_id).update(status=_REVIEW_STATUS_DELETED, updated_at=timezone.now())

    next_url = (request.POST.get('next') or '').strip()
    if next_url and url_has_allowed_host_and_scheme(
        url=next_url, allowed_hosts={request.get_host()}, require_https=request.is_secure(),
    ):
        return redirect(next_url)
    return redirect('/adverts/')


def review_publish(request: HttpRequest, review_id: int) -> HttpResponse:
    """Publish a review (admin only)."""
    if request.method != 'POST':
        return redirect('/adverts/')

    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    Review.objects.filter(pk=review_id).update(status=_REVIEW_STATUS_PUBLISHED, updated_at=timezone.now())

    next_url = (request.POST.get('next') or '').strip()
    if next_url and url_has_allowed_host_and_scheme(
        url=next_url, allowed_hosts={request.get_host()}, require_https=request.is_secure(),
    ):
        return redirect(next_url)
    return redirect('/adverts/')


def review_hide(request: HttpRequest, review_id: int) -> HttpResponse:
    """Hide a review (admin only)."""
    if request.method != 'POST':
        return redirect('/adverts/')

    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    Review.objects.filter(pk=review_id).update(status=_REVIEW_STATUS_HIDDEN, updated_at=timezone.now())

    next_url = (request.POST.get('next') or '').strip()
    if next_url and url_has_allowed_host_and_scheme(
        url=next_url, allowed_hosts={request.get_host()}, require_https=request.is_secure(),
    ):
        return redirect(next_url)
    return redirect('/adverts/')


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------

def _send_new_message_email(recipient_email: str, sender_name: str, advert_title: str, inbox_url: str) -> bool:
    subject = 'Новое сообщение на сайте'
    body = (
        f'{sender_name} отправил вам сообщение'
        + (f' по объявлению «{advert_title}»' if advert_title else '')
        + f'.\n\nПрочитайте его в личном кабинете:\n{inbox_url}\n'
    )
    return _send_email(recipient_email, subject, body)


def messages_inbox(request: HttpRequest) -> HttpResponse:
    """Show inbox: list of conversations grouped by the other party."""
    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/?next=/messages/')

    # Get all messages involving this user, newest first
    qs = Message.objects.filter(
        Q(sender_id=user.id) | Q(recipient_id=user.id)
    ).select_related('sender', 'recipient', 'advert').order_by('-created_at')

    # Group into conversations by the other user
    conversations: dict[int, dict] = {}
    for msg in qs[:500]:
        other_id = msg.recipient_id if msg.sender_id == user.id else msg.sender_id
        if other_id not in conversations:
            other_user = msg.recipient if msg.sender_id == user.id else msg.sender
            conversations[other_id] = {
                'other_user': other_user,
                'last_message': msg,
                'unread_count': 0,
            }
        if msg.recipient_id == user.id and not msg.is_read:
            conversations[other_id]['unread_count'] += 1

    conv_list = sorted(conversations.values(), key=lambda c: c['last_message'].created_at, reverse=True)

    resp = render(request, 'legacy/messages_inbox.html', {
        'legacy_user': user,
        'conversations': conv_list,
    })
    return _no_store(resp)


def messages_thread(request: HttpRequest, user_id: int) -> HttpResponse:
    """Show conversation thread with a specific user."""
    user = _get_current_legacy_user(request)
    if not user:
        return redirect(f"/login/?next=/messages/{user_id}/")

    other_user = get_object_or_404(LegacyUser, pk=user_id)

    messages_qs = Message.objects.filter(
        (Q(sender_id=user.id, recipient_id=user_id) | Q(sender_id=user_id, recipient_id=user.id))
    ).select_related('sender', 'recipient', 'advert').order_by('created_at')

    messages_list = list(messages_qs[:200])

    # Mark unread messages as read
    unread_ids = [m.id for m in messages_list if m.recipient_id == user.id and not m.is_read]
    if unread_ids:
        Message.objects.filter(pk__in=unread_ids).update(is_read=True)

    resp = render(request, 'legacy/messages_thread.html', {
        'legacy_user': user,
        'other_user': other_user,
        'messages': messages_list,
    })
    return _no_store(resp)


def message_send(request: HttpRequest) -> HttpResponse:
    """Send a message to another user."""
    if request.method != 'POST':
        return redirect('/messages/')

    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/')

    recipient_id_raw = (request.POST.get('recipient_id') or '').strip()
    text = (request.POST.get('text') or '').strip()
    advert_id_raw = (request.POST.get('advert_id') or '').strip()

    try:
        recipient_id = int(recipient_id_raw)
    except Exception:
        return redirect('/messages/')

    if recipient_id == user.id:
        return redirect('/messages/')

    recipient = LegacyUser.objects.filter(pk=recipient_id).first()
    if not recipient:
        return redirect('/messages/')

    if not text:
        return redirect(f"/messages/{recipient_id}/")

    if len(text) > 5000:
        text = text[:5000]

    advert_id = None
    advert_title = ''
    try:
        if advert_id_raw:
            advert_id = int(advert_id_raw)
            advert_obj = Advert.objects.filter(pk=advert_id).first()
            advert_title = (getattr(advert_obj, 'title', '') or '') if advert_obj else ''
    except Exception:
        advert_id = None

    now = timezone.now()
    Message.objects.create(
        sender_id=int(user.id),
        recipient_id=recipient_id,
        advert_id=advert_id,
        text=text,
        is_read=False,
        created_at=now,
    )

    # Email notification to recipient
    try:
        recipient_email = (getattr(recipient, 'email', '') or '').strip()
        sender_name = (getattr(user, 'name', '') or getattr(user, 'username', '') or '').strip()
        inbox_url = request.build_absolute_uri(f"/messages/{user.id}/")
        if recipient_email:
            _send_new_message_email(recipient_email, sender_name, advert_title, inbox_url)
    except Exception:
        logger.exception('Failed to send new message email notification')

    return redirect(f"/messages/{recipient_id}/")


def messages_unread_count_api(request: HttpRequest) -> JsonResponse:
    """API endpoint returning unread message count for the current user."""
    user = _get_current_legacy_user(request)
    if not user:
        return JsonResponse({'ok': False, 'count': 0})
    count = Message.objects.filter(recipient_id=user.id, is_read=False).count()
    return JsonResponse({'ok': True, 'count': count})
