from django.contrib.gis.geos import Point
from django.core.paginator import Paginator
from django.db.models import F
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.utils.http import url_has_allowed_host_and_scheme

from ..models import Advert, LegacyUser, Message
from ..constants import (
    ADVERT_STATUS_DELETED, ADVERT_STATUS_HIDDEN, ADVERT_STATUS_MODERATION,
    ADVERT_STATUS_PUBLISHED,
)
from .helpers import (
    _get_current_legacy_user, _is_admin_user,
    _annotate_adverts, _update_advert_status,
)

ADVERTS_PER_PAGE = 100


def _safe_page_num(raw) -> int:
    try:
        n = int(str(raw or '1').strip())
        return max(1, n)
    except (TypeError, ValueError):
        return 1


def _my_adverts_page(request, user):
    """(adverts, page, paginator) собственных объявлений пользователя."""
    my_qs = (
        Advert.objects.filter(author_id=user.id)
        .exclude(status=ADVERT_STATUS_DELETED)
        .select_related('category')
        .order_by('-updated_at', '-id')
    )
    my_paginator = Paginator(my_qs, ADVERTS_PER_PAGE)
    my_page = my_paginator.get_page(_safe_page_num(request.GET.get('page_my')))
    my_adverts = _annotate_adverts(list(my_page.object_list), user=user)
    return my_adverts, my_page, my_paginator


def _admin_filters(request):
    """(status, sort) админ-вкладки с фолбэком на 'all'/'created'."""
    admin_status = (request.GET.get('status') or 'all').strip().lower()
    admin_sort = (request.GET.get('sort') or 'created').strip().lower()
    if admin_status not in {'all', 'published', 'moderation', 'hidden'}:
        admin_status = 'all'
    if admin_sort not in {'created', 'hidden'}:
        admin_sort = 'created'
    return admin_status, admin_sort


def _admin_adverts_page(request, admin_status, admin_sort):
    """(adverts, page, paginator) админ-вкладки с фильтрами и меткой автора."""
    qs = Advert.objects.select_related('category', 'author').exclude(status=ADVERT_STATUS_DELETED)
    if admin_status == 'published':
        qs = qs.filter(status=ADVERT_STATUS_PUBLISHED)
    elif admin_status == 'moderation':
        qs = qs.filter(status=ADVERT_STATUS_MODERATION)
    elif admin_status == 'hidden':
        qs = qs.filter(status=ADVERT_STATUS_HIDDEN)

    if admin_sort == 'hidden':
        qs = qs.order_by(F('hidden_at').desc(nulls_last=True), '-id')
    else:
        qs = qs.order_by('-updated_at', '-id')

    admin_paginator = Paginator(qs, ADVERTS_PER_PAGE)
    admin_page = admin_paginator.get_page(_safe_page_num(request.GET.get('page_admin')))
    admin_adverts = _annotate_adverts(list(admin_page.object_list))
    for a in admin_adverts:
        try:
            a.author_label = str(getattr(getattr(a, 'author', None), 'username', '') or '')
        except Exception:
            a.author_label = ''
    return admin_adverts, admin_page, admin_paginator


def _parse_profile_coords(post):
    """(lat, lon) в допустимых диапазонах либо (None, None)."""
    lat_raw = (post.get('lat') or '').strip().replace(',', '.')
    lon_raw = (post.get('lon') or '').strip().replace(',', '.')
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
    return lat, lon


def _validate_profile(username, email, user):
    """Ошибки валидации логина/email (уникальность без самого юзера)."""
    errors = {}
    if not username:
        errors['username'] = 'Введите логин'
    elif len(username) > 255:
        errors['username'] = 'Логин слишком длинный (макс. 255 символов)'
    elif LegacyUser.objects.filter(username=username).exclude(pk=user.pk).exists():
        errors['username'] = 'Этот логин уже занят'

    if not email:
        errors['email'] = 'Введите email'
    elif LegacyUser.objects.filter(email=email).exclude(pk=user.pk).exists():
        errors['email'] = 'Этот email уже занят'
    return errors


def _contacts_with_show_address(user, show_address_new):
    """Переписать флаг show_address внутри строки contacts."""
    contacts_new = str(getattr(user, 'contacts', '') or '')
    contacts_new = contacts_new.replace('show_address=1', '')
    if show_address_new:
        contacts_new = contacts_new.replace('show_address=0', '')
    else:
        if 'show_address=0' not in contacts_new:
            contacts_new = (contacts_new + ('; ' if contacts_new.strip() else '') + 'show_address=0').strip()

    contacts_new = contacts_new.replace(';;', ';')
    return contacts_new.strip(' ;\n\t')


def _save_profile(user, post):
    """Валидация и сохранение формы профиля → (errors, saved, show_address)."""
    username = (post.get('username') or '').strip()
    email = (post.get('email') or '').strip()
    show_address_raw = (post.get('show_address') or '').strip().lower()
    show_address_new = show_address_raw in {'1', 'true', 'yes', 'on'}

    lat, lon = _parse_profile_coords(post)

    errors = _validate_profile(username, email, user)
    if errors:
        return errors, False, None

    update_fields = ['username', 'email', 'name', 'phone', 'address', 'contacts', 'updated_at']
    user.username = username
    user.email = email
    user.name = (post.get('name') or '').strip()
    user.phone = (post.get('phone') or '').strip()
    user.address = (post.get('address') or '').strip()
    user.contacts = _contacts_with_show_address(user, show_address_new)
    user.updated_at = timezone.now()
    if lat is not None and lon is not None:
        user.location = Point(float(lon), float(lat), srid=4326)
        update_fields.append('location')
    user.save(update_fields=update_fields)
    return errors, True, show_address_new


def legacy_me(request: HttpRequest) -> HttpResponse:
    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/?next=/me/')

    errors = {}
    saved = False

    is_admin = _is_admin_user(user)

    my_adverts, my_page, my_paginator = _my_adverts_page(request, user)

    admin_status, admin_sort = _admin_filters(request)
    admin_adverts = []
    admin_page = None
    admin_paginator = None
    if is_admin:
        admin_adverts, admin_page, admin_paginator = _admin_adverts_page(
            request, admin_status, admin_sort,
        )

    current_contacts = str(getattr(user, 'contacts', '') or '')
    show_address_enabled = ('show_address=0' not in current_contacts)

    if request.method == 'POST':
        errors, saved, show_address_new = _save_profile(user, request.POST)
        if saved:
            show_address_enabled = show_address_new

    # Which tab is active on initial render.  Client-side JS also reacts
    # to URL hash (#credentials / #adverts) for in-page tab switching.
    section = (request.GET.get('section') or '').strip().lower()
    if section not in {'credentials', 'adverts'}:
        section = 'credentials'

    return render(
        request,
        'legacy/me.html',
        {
            'legacy_user': user,
            'errors': errors,
            'saved': saved,
            'show_address_enabled': show_address_enabled,
            'my_adverts': my_adverts,
            'my_page': my_page,
            'my_paginator': my_paginator,
            'is_admin_cabinet': is_admin,
            'admin_adverts': admin_adverts,
            'admin_page': admin_page,
            'admin_paginator': admin_paginator,
            'admin_filter_status': admin_status,
            'admin_filter_sort': admin_sort,
            'active_section': section,
            'messages_unread_count': Message.objects.filter(
                recipient_id=user.id, is_read=False,
            ).count(),
        },
    )


def _parse_bulk_ids(ids_raw):
    """Список уникальных положительных id; мусор отбрасывается."""
    ids: list[int] = []
    for x in ids_raw:
        try:
            v = int(str(x).strip())
        except Exception:
            continue
        if v > 0:
            ids.append(v)
    return sorted(set(ids))


def _apply_bulk_action(action, ids, user, is_admin):
    """Применить действие к каждому доступному пользователю объявлению."""
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
            _update_advert_status(advert_id, ADVERT_STATUS_HIDDEN)
        elif action == 'delete':
            _update_advert_status(advert_id, ADVERT_STATUS_DELETED)
        elif action == 'publish':
            _update_advert_status(advert_id, ADVERT_STATUS_PUBLISHED)
        elif action == 'bump':
            if int(getattr(obj, 'status', 0) or 0) == ADVERT_STATUS_PUBLISHED:
                Advert.objects.filter(pk=advert_id).update(created_at=now, updated_at=now)


def _bulk_redirect(request):
    """Редирект на безопасный next либо на /me/."""
    next_raw = (request.POST.get('next') or '').strip()
    if next_raw and url_has_allowed_host_and_scheme(
        url=next_raw,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        return redirect(next_raw)
    return redirect('/me/')


def legacy_me_bulk_adverts(request: HttpRequest) -> HttpResponse:
    if request.method != 'POST':
        return redirect('/me/')

    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/?next=/me/')

    action = (request.POST.get('action') or '').strip().lower()
    ids = _parse_bulk_ids(request.POST.getlist('advert_id'))
    if not ids:
        return redirect('/me/')

    is_admin = _is_admin_user(user)
    if action not in {'hide', 'delete', 'bump', 'publish'}:
        return redirect('/me/')
    if action == 'publish' and not is_admin:
        return redirect('/me/')

    _apply_bulk_action(action, ids, user, is_admin)

    return _bulk_redirect(request)
