"""
Собственный чат поддержки: виджет на сайте ↔ бот в мессенджере (MAX и/или Telegram).

Поток:
  посетитель пишет в виджет → ``SupportMessage(direction='in')`` →
  ``notify_operators`` шлёт текст ботом в чат поддержки (``TG_SUPPORT_CHAT_ID``)
  и запоминает ``message_id`` Telegram;
  оператор делает Reply на это сообщение → Telegram вызывает webhook →
  ``handle_telegram_update`` находит тред по ``tg_message_id`` (или по
  ``#<id>`` в цитируемом тексте) → ``SupportMessage(direction='out')`` →
  виджет забирает ответ polling'ом. MAX работает так же (``handle_max_update``,
  ``max_message_id``); оба транспорта могут быть включены одновременно.

MAX Bot API (dev.max.ru) живёт за сертификатом Минцифры, которого нет в certifi,
поэтому запросы идут с объединённым CA-bundle (certifi + deploy/certs/*.pem).

Посетитель идентифицируется cookie ``VISITOR_COOKIE`` (случайный ключ),
логин не требуется; если пользователь залогинен — тред привязывается к нему.
"""
import hashlib
import logging
import os
import re
import secrets
import tempfile
from datetime import timedelta
from pathlib import Path

import certifi
import requests
from django.conf import settings
from django.utils import timezone

from .models import SupportMessage, SupportThread

logger = logging.getLogger(__name__)

VISITOR_COOKIE = 'sc_vid'
VISITOR_COOKIE_MAX_AGE = 365 * 24 * 3600
MAX_TEXT_LEN = 2000
RATE_LIMIT_COUNT = 20
RATE_LIMIT_WINDOW = timedelta(minutes=10)
TG_API = 'https://api.telegram.org/bot{token}/{method}'
MAX_API = 'https://platform-api2.max.ru'
MAX_SECRET_HEADER = 'HTTP_X_MAX_BOT_API_SECRET'
_RU_CERTS_DIR = Path(__file__).resolve().parent.parent / 'deploy' / 'certs'
_THREAD_REF_RE = re.compile(r'#(\d+)')


# ── конфигурация ──────────────────────────────────────────────────────────

def tg_configured() -> bool:
    return bool(settings.TG_SUPPORT_BOT_TOKEN and settings.TG_SUPPORT_CHAT_ID)


def max_configured() -> bool:
    return bool(settings.MAX_SUPPORT_BOT_TOKEN and settings.MAX_SUPPORT_CHAT_ID)


def is_configured() -> bool:
    return tg_configured() or max_configured()


def _derived_secret(prefix: str) -> str:
    return hashlib.sha256(f'{prefix}:{settings.SECRET_KEY}'.encode()).hexdigest()[:40]


def webhook_secret() -> str:
    return getattr(settings, 'TG_SUPPORT_WEBHOOK_SECRET', '') or _derived_secret('tg-support')


def webhook_path() -> str:
    return f'/api/support/tg/{webhook_secret()}/'


def max_webhook_secret() -> str:
    return getattr(settings, 'MAX_SUPPORT_WEBHOOK_SECRET', '') or _derived_secret('max-support')


def max_webhook_path() -> str:
    return f'/api/support/max/{max_webhook_secret()}/'


# ── тред посетителя ───────────────────────────────────────────────────────

def new_visitor_key() -> str:
    return secrets.token_hex(16)


def get_thread(visitor_key: str | None) -> SupportThread | None:
    if not visitor_key or len(visitor_key) > 64:
        return None
    return SupportThread.objects.filter(visitor_key=visitor_key).first()


def get_or_create_thread(visitor_key: str | None, *, user=None, page_url='', user_agent='',
                         ip_address=None) -> tuple[SupportThread, str]:
    """Вернуть (тред, visitor_key). Ключ создаётся, если cookie нет/невалидна."""
    thread = get_thread(visitor_key)
    if thread is None:
        visitor_key = new_visitor_key()
        thread = SupportThread.objects.create(
            visitor_key=visitor_key, user=user, page_url=page_url[:500],
            user_agent=user_agent[:300], ip_address=ip_address or None,
        )
        return thread, visitor_key

    changed = []
    if user is not None and thread.user_id != user.pk:
        thread.user = user
        changed.append('user')
    if page_url and thread.page_url != page_url[:500]:
        thread.page_url = page_url[:500]
        changed.append('page_url')
    if changed:
        thread.save(update_fields=changed + ['updated_at'])
    return thread, visitor_key


def rate_limited(thread: SupportThread) -> bool:
    since = timezone.now() - RATE_LIMIT_WINDOW
    recent = SupportMessage.objects.filter(
        thread=thread, direction=SupportMessage.DIRECTION_IN, created_at__gte=since,
    ).count()
    return recent >= RATE_LIMIT_COUNT


def add_visitor_message(thread: SupportThread, text: str) -> SupportMessage:
    msg = SupportMessage.objects.create(
        thread=thread, direction=SupportMessage.DIRECTION_IN, text=text,
    )
    thread.last_visitor_message_at = msg.created_at
    thread.save(update_fields=['last_visitor_message_at', 'updated_at'])
    notify_operators(msg)
    return msg


def add_operator_message(thread: SupportThread, text: str, tg_message_id: int | None = None) -> SupportMessage:
    msg = SupportMessage.objects.create(
        thread=thread, direction=SupportMessage.DIRECTION_OUT, text=text, tg_message_id=tg_message_id,
    )
    thread.last_operator_message_at = msg.created_at
    thread.save(update_fields=['last_operator_message_at', 'updated_at'])
    return msg


def serialize_message(msg: SupportMessage) -> dict:
    return {
        'id': msg.id,
        'dir': msg.direction,
        'text': msg.text,
        'at': timezone.localtime(msg.created_at).strftime('%d.%m %H:%M'),
    }


# ── Telegram Bot API ──────────────────────────────────────────────────────

def tg_call(method: str, **params) -> dict | None:
    token = settings.TG_SUPPORT_BOT_TOKEN
    if not token:
        return None
    try:
        resp = requests.post(TG_API.format(token=token, method=method), json=params, timeout=10)
        data = resp.json()
    except Exception:
        logger.exception('Telegram API %s failed', method)
        return None
    if not data.get('ok'):
        logger.error('Telegram API %s error: %s', method, data.get('description'))
        return None
    return data.get('result')


def _operator_text(msg: SupportMessage) -> str:
    thread = msg.thread
    who = thread.visitor_label
    if thread.user_id and thread.user:
        who += f' (логин {thread.user.username}'
        if thread.user.email:
            who += f', {thread.user.email}'
        who += ')'
    lines = [f'💬 Обращение #{thread.id} — {who}']
    if thread.page_url:
        lines.append(f'Страница: {thread.page_url}')
    lines.append('')
    lines.append(msg.text)
    lines.append('')
    lines.append('↩️ Ответьте на это сообщение (Reply), чтобы посетитель увидел ответ на сайте.')
    return '\n'.join(lines)[:4000]


def notify_operators(msg: SupportMessage) -> bool:
    """Переслать сообщение посетителя во все настроенные мессенджеры; сохранить id отправленных."""
    if not is_configured():
        logger.warning('Support chat: no messenger bot configured; message #%s kept in DB only', msg.id)
        return False
    text = _operator_text(msg)
    update_fields = []
    if max_configured():
        result = max_call('POST', '/messages', params={'chat_id': settings.MAX_SUPPORT_CHAT_ID},
                          json={'text': text, 'notify': True})
        mid = (((result or {}).get('message') or {}).get('body') or {}).get('mid')
        if mid:
            msg.max_message_id = str(mid)[:64]
            update_fields.append('max_message_id')
    if tg_configured():
        result = tg_call(
            'sendMessage', chat_id=settings.TG_SUPPORT_CHAT_ID, text=text,
            disable_web_page_preview=True,
        )
        if result:
            msg.tg_message_id = result.get('message_id')
            update_fields.append('tg_message_id')
    if update_fields:
        msg.save(update_fields=update_fields)
    return bool(update_fields)


def _thread_from_reply(reply: dict) -> SupportThread | None:
    tg_id = reply.get('message_id')
    if tg_id:
        linked = (
            SupportMessage.objects.filter(tg_message_id=tg_id)
            .select_related('thread').first()
        )
        if linked:
            return linked.thread
    m = _THREAD_REF_RE.search(reply.get('text') or '')
    if m:
        return SupportThread.objects.filter(pk=int(m.group(1))).first()
    return None


def handle_telegram_update(update: dict) -> str:
    """Обработать update от Telegram. Возвращает код результата (для логов/тестов)."""
    message = update.get('message') or update.get('edited_message')
    if not message:
        return 'ignored:no_message'
    chat_id = str((message.get('chat') or {}).get('id', ''))
    if not settings.TG_SUPPORT_CHAT_ID or chat_id != str(settings.TG_SUPPORT_CHAT_ID):
        return 'ignored:foreign_chat'
    text = (message.get('text') or '').strip()
    if not text or text.startswith('/'):
        return 'ignored:empty_or_command'

    reply = message.get('reply_to_message')
    thread = _thread_from_reply(reply) if reply else None
    if thread is None:
        tg_call(
            'sendMessage', chat_id=chat_id,
            text='Чтобы ответить посетителю, используйте «Ответить» (Reply) на его сообщение.',
            reply_to_message_id=message.get('message_id'),
        )
        return 'no_thread'

    if SupportMessage.objects.filter(tg_message_id=message.get('message_id'),
                                     direction=SupportMessage.DIRECTION_OUT).exists():
        return 'duplicate'
    add_operator_message(thread, text[:MAX_TEXT_LEN], tg_message_id=message.get('message_id'))
    return 'ok'


# ── MAX Bot API ────────────────────────────────────────────────────────────────────────

_ca_bundle_path: str | None = None


def ca_bundle() -> str:
    """Путь к CA-bundle = certifi + сертификаты Минцифры из deploy/certs (собирается один раз)."""
    global _ca_bundle_path
    if _ca_bundle_path and os.path.exists(_ca_bundle_path):
        return _ca_bundle_path
    extra = sorted(_RU_CERTS_DIR.glob('*.pem')) if _RU_CERTS_DIR.is_dir() else []
    if not extra:
        _ca_bundle_path = certifi.where()
        return _ca_bundle_path
    parts = [Path(certifi.where()).read_bytes()] + [p.read_bytes() for p in extra]
    digest = hashlib.sha1(b''.join(parts)).hexdigest()[:12]
    target = Path(tempfile.gettempdir()) / f'enb-ca-bundle-{digest}.pem'
    if not target.exists():
        tmp = target.with_suffix(f'.{os.getpid()}.tmp')
        tmp.write_bytes(b'\n'.join(parts))
        os.replace(tmp, target)
    _ca_bundle_path = str(target)
    return _ca_bundle_path


def max_call(method: str, path: str, *, params: dict | None = None, json: dict | None = None) -> dict | None:
    """Вызов MAX Bot API. Возвращает тело ответа (dict) или None при ошибке."""
    token = settings.MAX_SUPPORT_BOT_TOKEN
    if not token:
        return None
    try:
        resp = requests.request(
            method, MAX_API + path, params=params, json=json, timeout=10,
            headers={'Authorization': token}, verify=ca_bundle(),
        )
    except Exception:
        logger.exception('MAX API %s %s failed', method, path)
        return None
    try:
        data = resp.json()
    except ValueError:
        data = {}
    if resp.status_code >= 400:
        logger.error('MAX API %s %s -> %s: %s', method, path, resp.status_code,
                     (data or {}).get('message') or resp.text[:200])
        return None
    return data if isinstance(data, dict) else {}


def _thread_from_max_link(link: dict) -> SupportThread | None:
    quoted = link.get('message') or {}
    mid = quoted.get('mid')
    if mid:
        linked = (
            SupportMessage.objects.filter(max_message_id=str(mid))
            .select_related('thread').first()
        )
        if linked:
            return linked.thread
    m = _THREAD_REF_RE.search(quoted.get('text') or '')
    if m:
        return SupportThread.objects.filter(pk=int(m.group(1))).first()
    return None


def handle_max_update(update: dict) -> str:
    """Обработать Update от MAX (message_created). Возвращает код результата (для логов/тестов)."""
    if update.get('update_type') != 'message_created':
        return 'ignored:type'
    message = update.get('message') or {}
    if (message.get('sender') or {}).get('is_bot'):
        return 'ignored:bot'
    chat_id = str((message.get('recipient') or {}).get('chat_id', ''))
    if not settings.MAX_SUPPORT_CHAT_ID or chat_id != str(settings.MAX_SUPPORT_CHAT_ID):
        return 'ignored:foreign_chat'
    body = message.get('body') or {}
    text = (body.get('text') or '').strip()
    if not text or text.startswith('/'):
        return 'ignored:empty_or_command'
    mid = str(body.get('mid') or '')[:64]

    link = message.get('link') or {}
    thread = _thread_from_max_link(link) if link.get('type') == 'reply' else None
    if thread is None:
        payload = {'text': 'Чтобы ответить посетителю, используйте «Ответить» (Reply) на его сообщение.'}
        if mid:
            payload['link'] = {'type': 'reply', 'mid': mid}
        max_call('POST', '/messages', params={'chat_id': chat_id}, json=payload)
        return 'no_thread'

    if mid and SupportMessage.objects.filter(max_message_id=mid, direction=SupportMessage.DIRECTION_OUT).exists():
        return 'duplicate'
    msg = add_operator_message(thread, text[:MAX_TEXT_LEN])
    if mid:
        msg.max_message_id = mid
        msg.save(update_fields=['max_message_id'])
    return 'ok'
