"""OAuth views for VK and OK (Одноклассники) social login."""
import hashlib
import logging
import secrets
import urllib.parse

import requests
from django.conf import settings
from django.contrib.auth.hashers import make_password
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone

from ..constants import USER_STATUS_ACTIVE
from ..models import LegacyUser, SocialAccount
from .helpers import _get_current_legacy_user, _no_store

logger = logging.getLogger(__name__)

# ─── helpers ──────────────────────────────────────────────────────────────

def _login_social_user(request: HttpRequest, user: LegacyUser, next_url: str = '') -> HttpResponse:
    """Set session and redirect after successful social auth."""
    request.session['legacy_user_id'] = int(user.id)
    return redirect(next_url or '/adverts/')


def _find_or_start_social(request, provider, provider_uid, profile: dict):
    """
    If SocialAccount exists — log in.
    Otherwise store profile in session and redirect to complete-profile page.
    """
    try:
        sa = SocialAccount.objects.select_related('user').get(
            provider=provider, provider_uid=str(provider_uid),
        )
        next_url = request.session.pop('oauth_next', '')
        return _login_social_user(request, sa.user, next_url)
    except SocialAccount.DoesNotExist:
        pass

    # Check if user with same email already exists — link account
    email = (profile.get('email') or '').strip().lower()
    if email:
        try:
            existing = LegacyUser.objects.get(email=email)
            SocialAccount.objects.create(
                user=existing,
                provider=provider,
                provider_uid=str(provider_uid),
                access_token=profile.get('access_token', ''),
                extra_data=profile,
            )
            next_url = request.session.pop('oauth_next', '')
            return _login_social_user(request, existing, next_url)
        except LegacyUser.DoesNotExist:
            pass

    # New user — save to session, redirect to complete profile
    request.session['oauth_profile'] = {
        'provider': provider,
        'provider_uid': str(provider_uid),
        'name': profile.get('name', ''),
        'email': email,
        'phone': profile.get('phone', ''),
        'access_token': profile.get('access_token', ''),
        'extra_data': profile,
    }
    request.session.modified = True
    return redirect('/oauth/complete/')


# ─── VK ──────────────────────────────────────────────────────────────────

def oauth_vk_start(request: HttpRequest) -> HttpResponse:
    """Redirect user to VK authorization page."""
    next_url = (request.GET.get('next') or '').strip()
    if next_url:
        request.session['oauth_next'] = next_url

    state = secrets.token_urlsafe(16)
    request.session['oauth_state'] = state

    params = {
        'client_id': settings.VK_CLIENT_ID,
        'redirect_uri': settings.VK_REDIRECT_URI,
        'display': 'page',
        'scope': 'email',
        'response_type': 'code',
        'state': state,
        'v': '5.131',
    }
    url = 'https://oauth.vk.com/authorize?' + urllib.parse.urlencode(params)
    return redirect(url)


def oauth_vk_callback(request: HttpRequest) -> HttpResponse:
    """Handle VK OAuth callback."""
    error = request.GET.get('error')
    if error:
        logger.warning('VK OAuth error: %s — %s', error, request.GET.get('error_description', ''))
        return redirect('/register/')

    code = request.GET.get('code', '')
    state = request.GET.get('state', '')

    if not code or state != request.session.pop('oauth_state', ''):
        logger.warning('VK OAuth: missing code or state mismatch')
        return redirect('/register/')

    # Exchange code for access_token
    try:
        resp = requests.post('https://oauth.vk.com/access_token', data={
            'client_id': settings.VK_CLIENT_ID,
            'client_secret': settings.VK_CLIENT_SECRET,
            'redirect_uri': settings.VK_REDIRECT_URI,
            'code': code,
        }, timeout=10)
        data = resp.json()
    except Exception:
        logger.exception('VK OAuth token exchange failed')
        return redirect('/register/')

    access_token = data.get('access_token', '')
    vk_user_id = data.get('user_id')
    email = data.get('email', '')

    if not vk_user_id:
        logger.warning('VK OAuth: no user_id in token response: %s', data)
        return redirect('/register/')

    # Fetch user profile
    name = ''
    try:
        profile_resp = requests.get('https://api.vk.com/method/users.get', params={
            'user_ids': vk_user_id,
            'fields': 'first_name,last_name,photo_200',
            'access_token': access_token,
            'v': '5.131',
        }, timeout=10)
        profile_data = profile_resp.json()
        users = profile_data.get('response', [])
        if users:
            u = users[0]
            name = f"{u.get('first_name', '')} {u.get('last_name', '')}".strip()
    except Exception:
        logger.exception('VK profile fetch failed')

    profile = {
        'name': name,
        'email': email or '',
        'phone': '',
        'access_token': access_token,
        'photo': '',
    }
    return _find_or_start_social(request, SocialAccount.PROVIDER_VK, str(vk_user_id), profile)


# ─── OK (Одноклассники) ─────────────────────────────────────────────────

def oauth_ok_start(request: HttpRequest) -> HttpResponse:
    """Redirect user to OK authorization page."""
    next_url = (request.GET.get('next') or '').strip()
    if next_url:
        request.session['oauth_next'] = next_url

    state = secrets.token_urlsafe(16)
    request.session['oauth_state'] = state

    params = {
        'client_id': settings.OK_CLIENT_ID,
        'redirect_uri': settings.OK_REDIRECT_URI,
        'scope': 'VALUABLE_ACCESS;GET_EMAIL',
        'response_type': 'code',
        'state': state,
        'layout': 'w',
    }
    url = 'https://connect.ok.ru/oauth/authorize?' + urllib.parse.urlencode(params)
    return redirect(url)


def _ok_sig(params: dict, access_token: str) -> str:
    """Calculate OK API request signature."""
    secret_key = hashlib.md5(
        (access_token + settings.OK_CLIENT_SECRET).encode()
    ).hexdigest()
    sorted_params = ''.join(f'{k}={params[k]}' for k in sorted(params))
    return hashlib.md5((sorted_params + secret_key).encode()).hexdigest()


def oauth_ok_callback(request: HttpRequest) -> HttpResponse:
    """Handle OK OAuth callback."""
    error = request.GET.get('error')
    if error:
        logger.warning('OK OAuth error: %s', error)
        return redirect('/register/')

    code = request.GET.get('code', '')
    state = request.GET.get('state', '')

    if not code or state != request.session.pop('oauth_state', ''):
        logger.warning('OK OAuth: missing code or state mismatch')
        return redirect('/register/')

    # Exchange code for access_token
    try:
        resp = requests.post('https://api.ok.ru/oauth/token.do', data={
            'client_id': settings.OK_CLIENT_ID,
            'client_secret': settings.OK_CLIENT_SECRET,
            'redirect_uri': settings.OK_REDIRECT_URI,
            'code': code,
            'grant_type': 'authorization_code',
        }, timeout=10)
        data = resp.json()
    except Exception:
        logger.exception('OK OAuth token exchange failed')
        return redirect('/register/')

    access_token = data.get('access_token', '')
    if not access_token:
        logger.warning('OK OAuth: no access_token: %s', data)
        return redirect('/register/')

    # Fetch user profile
    api_params = {
        'application_key': settings.OK_PUBLIC_KEY,
        'format': 'json',
        'method': 'users.getCurrentUser',
        'fields': 'uid,name,first_name,last_name,email,pic_2',
    }
    api_params['sig'] = _ok_sig(api_params, access_token)
    api_params['access_token'] = access_token

    try:
        profile_resp = requests.get('https://api.ok.ru/fb.do', params=api_params, timeout=10)
        profile_data = profile_resp.json()
    except Exception:
        logger.exception('OK profile fetch failed')
        return redirect('/register/')

    ok_uid = str(profile_data.get('uid', ''))
    if not ok_uid:
        logger.warning('OK OAuth: no uid in profile: %s', profile_data)
        return redirect('/register/')

    name = profile_data.get('name', '')
    email = profile_data.get('email', '')

    profile = {
        'name': name,
        'email': email or '',
        'phone': '',
        'access_token': access_token,
    }
    return _find_or_start_social(request, SocialAccount.PROVIDER_OK, ok_uid, profile)


# ─── Complete profile ────────────────────────────────────────────────────

def oauth_complete(request: HttpRequest) -> HttpResponse:
    """Page where social-auth user fills in missing username/email."""
    oauth_profile = request.session.get('oauth_profile')
    if not oauth_profile:
        return redirect('/register/')

    errors = {}

    if request.method == 'POST':
        username = (request.POST.get('username') or '').strip()
        email = (request.POST.get('email') or '').strip().lower()
        name = (request.POST.get('name') or '').strip()
        phone = (request.POST.get('phone') or '').strip()

        if not username:
            errors['username'] = 'Введите username'
        if not email:
            errors['email'] = 'Введите email'

        if username and LegacyUser.objects.filter(username=username).exists():
            errors['username'] = 'Этот username уже занят'
        if email and LegacyUser.objects.filter(email=email).exists():
            errors['email'] = 'Этот email уже зарегистрирован'

        if not errors:
            now = timezone.now()
            pw_hash = make_password(secrets.token_urlsafe(12))
            auth_key = secrets.token_hex(16)[:32]

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

            SocialAccount.objects.create(
                user=new_user,
                provider=oauth_profile['provider'],
                provider_uid=oauth_profile['provider_uid'],
                access_token=oauth_profile.get('access_token', ''),
                extra_data=oauth_profile.get('extra_data', {}),
            )

            request.session.pop('oauth_profile', None)
            next_url = request.session.pop('oauth_next', '')
            return _login_social_user(request, new_user, next_url)

    # Pre-fill from social profile
    form = {
        'username': request.POST.get('username', '') if request.method == 'POST' else '',
        'email': request.POST.get('email', '') if request.method == 'POST' else (oauth_profile.get('email') or ''),
        'name': request.POST.get('name', '') if request.method == 'POST' else (oauth_profile.get('name') or ''),
        'phone': request.POST.get('phone', '') if request.method == 'POST' else (oauth_profile.get('phone') or ''),
    }

    provider_label = 'ВКонтакте' if oauth_profile['provider'] == 'vk' else 'Одноклассники'

    resp = render(request, 'legacy/oauth_complete.html', {
        'errors': errors,
        'form': form,
        'provider_label': provider_label,
        'legacy_user': _get_current_legacy_user(request),
    })
    return _no_store(resp)
