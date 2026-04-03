import datetime
import random
import secrets
import urllib.parse

from django.conf import settings
from django.contrib.auth import authenticate, login as django_login
from django.contrib.auth.hashers import check_password, make_password
from django.contrib.gis.geos import Point
from django.core import signing
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.utils.http import url_has_allowed_host_and_scheme

from ..models import LegacyUser
from ..constants import USER_STATUS_ACTIVE
from ..sms import send_otp
from .helpers import (
    _get_current_legacy_user, _no_store, _normalize_phone,
    _antispam_token, _antispam_check,
    _make_set_password_token, _send_registration_email,
    logger,
)


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
    ts, th = _antispam_token()
    if request.method == 'POST':
        spam = _antispam_check(request)
        if spam:
            return redirect('/register/sms/')

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
                    'antispam_ts': ts, 'antispam_th': th,
                },
            )
            return _no_store(resp)

        otp_code = f"{random.randint(0, 999999):06d}"

        if not send_otp(phone, otp_code):
            logger.warning('SMS not delivered to %s', phone)

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
            'antispam_ts': ts, 'antispam_th': th,
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

    reg_extra_contacts = []

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

        _REG_CONTACT_TYPES = {'email', 'telegram', 'max', 'social', 'website'}
        reg_extra_contacts = []
        ec_types = request.POST.getlist('ec_type')
        ec_values = request.POST.getlist('ec_value')
        for ect, ecv in zip(ec_types, ec_values):
            ect = (ect or '').strip()
            ecv = (ecv or '').strip()
            if ect in _REG_CONTACT_TYPES and ecv:
                reg_extra_contacts.append({'type': ect, 'value': ecv})

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
                status=USER_STATUS_ACTIVE,
                created_at=now,
                updated_at=now,
                contacts=contacts_val,
                extra_contacts=reg_extra_contacts or [],
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
                'extra_contacts': reg_extra_contacts,
            },
            'phone': phone,
            'dev_code': otp_code if settings.DEBUG else '',
        },
    )
    return _no_store(resp)


def legacy_register_email(request: HttpRequest) -> HttpResponse:
    ts, th = _antispam_token()
    if request.method == 'POST':
        spam = _antispam_check(request)
        if spam:
            return redirect('/register/email/')

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
                    'antispam_ts': ts, 'antispam_th': th,
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
            status=USER_STATUS_ACTIVE,
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
            'antispam_ts': ts, 'antispam_th': th,
        },
    )
    return _no_store(resp)


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
