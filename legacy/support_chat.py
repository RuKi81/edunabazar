"""
Собственный чат поддержки: виджет на сайте ↔ Telegram-бот.

Поток:
  посетитель пишет в виджет → ``SupportMessage(direction='in')`` →
  ``notify_operators`` шлёт текст ботом в чат поддержки (``TG_SUPPORT_CHAT_ID``)
  и запоминает ``message_id`` Telegram;
  оператор делает Reply на это сообщение → Telegram вызывает webhook →
  ``handle_telegram_update`` находит тред по ``tg_message_id`` (или по
  ``#<id>`` в цитируемом тексте) → ``SupportMessage(direction='out')`` →
  виджет забирает ответ polling'ом.

Посетитель идентифицируется cookie ``VISITOR_COOKIE`` (случайный ключ),
логин не требуется; если пользователь залогинен — тред привязывается к нему.
"""
import hashlib
import logging
import re
import secrets
from datetime import timedelta

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
_THREAD_REF_RE = re.compile(r'#(\d+)')


# ── конфигурация ──────────────────────────────────────────────────────────

def is_configured() -> bool:
    return bool(settings.TG_SUPPORT_BOT_TOKEN and settings.TG_SUPPORT_CHAT_ID)


def webhook_secret() -> str:
    explicit = getattr(settings, 'TG_SUPPORT_WEBHOOK_SECRET', '')
    if explicit:
        return explicit
    return hashlib.sha256(f'tg-support:{settings.SECRET_KEY}'.encode()).hexdigest()[:40]


def webhook_path() -> str:
    return f'/api/support/tg/{webhook_secret()}/'


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
    """Переслать сообщение посетителя в чат поддержки; сохранить tg message_id."""
    if not is_configured():
        logger.warning('Support chat: TG_SUPPORT_BOT_TOKEN/CHAT_ID not set; message #%s kept in DB only', msg.id)
        return False
    result = tg_call(
        'sendMessage', chat_id=settings.TG_SUPPORT_CHAT_ID, text=_operator_text(msg),
        disable_web_page_preview=True,
    )
    if not result:
        return False
    msg.tg_message_id = result.get('message_id')
    msg.save(update_fields=['tg_message_id'])
    return True


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
