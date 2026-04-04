"""
Management command: fetch one agricultural news article per day,
rewrite it via GigaChat (Sber, free for individuals), and save to the News table.

Usage:
    python manage.py fetch_news          # fetch & rewrite 1 article
    python manage.py fetch_news --dry    # preview without saving
    python manage.py fetch_news --count 3  # fetch 3 articles

Cron (daily at 07:00 Moscow time):
    0 7 * * * cd /opt/edunabazar && docker compose -f docker-compose.prod.yml exec -T web python manage.py fetch_news
"""

import hashlib
import logging
import re
import html as html_mod
import uuid
from datetime import date, datetime, timedelta

import feedparser
import requests
import urllib3
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from legacy.models import News, NewsFeedSource, NewsKeyword
from legacy.cache_utils import invalidate_home_cache

logger = logging.getLogger('legacy.fetch_news')

def _get_feed_sources() -> list[dict]:
    """Load RSS sources from DB (NewsFeedSource model)."""
    sources = list(
        NewsFeedSource.objects.filter(is_active=True).values('name', 'url')
    )
    return sources


def _get_keywords(keyword_type: str) -> list[str]:
    """Load keywords from DB (NewsKeyword model)."""
    return list(
        NewsKeyword.objects.filter(
            keyword_type=keyword_type, is_active=True
        ).values_list('keyword', flat=True)
    )

# ── GigaChat (Sber) — free for individuals ─────────────────────────
GIGACHAT_OAUTH_URL = 'https://ngw.devices.sberbank.ru:9443/api/v2/oauth'
GIGACHAT_API_URL = 'https://gigachat.devices.sberbank.ru/api/v1/chat/completions'
GIGACHAT_MODEL = 'GigaChat'
GIGACHAT_SCOPE = 'GIGACHAT_API_PERS'

_gigachat_token_cache: dict = {'token': '', 'expires': 0}


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r'<[^>]+>', '', text or '')
    text = html_mod.unescape(text)
    return text.strip()


def _url_hash(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


# Default keywords when DB has none — agriculture, food industry, agro machinery
_DEFAULT_INCLUDE_KW = [
    'сельское хозяйство', 'сельскохозяйствен', 'сельхоз', 'агропром',
    'агрокомплекс', 'агрохолдинг', 'агросектор', 'агробизнес', 'агротех',
    'растениеводство', 'животноводство', 'птицеводство', 'свиноводство',
    'молочн', 'зерно', 'зернов', 'пшениц', 'ячмень', 'кукуруз', 'подсолнечник',
    'соя', 'рапс', 'сахарн', 'свёкл', 'свекл', 'картофел', 'овощ', 'фрукт',
    'урожай', 'посев', 'уборк', 'жатв', 'удобрени', 'пестицид', 'гербицид',
    'комбайн', 'трактор', 'сеялк', 'плуг', 'борон', 'опрыскивател',
    'пищев', 'продовольств', 'мясопереработ', 'молокозавод', 'элеватор',
    'мукомольн', 'хлебозавод', 'маслозавод', 'сахарный завод',
    'минсельхоз', 'россельхоз', 'россельхознадзор',
    'фермер', 'аграри', 'аграрн', 'посевн', 'кормов',
    'теплиц', 'садоводств', 'виноградарств', 'рыбоводств', 'аквакультур',
]
_DEFAULT_EXCLUDE_KW = [
    'криптовалют', 'биткоин', 'блокчейн', 'нфт', 'nft',
    'футбол', 'хоккей', 'баскетбол', 'олимпи',
    'шоу-бизнес', 'знаменитост', 'селебрити', 'сериал',
    'гороскоп', 'астролог',
]


def _is_agro(title: str, summary: str, include_kw: list[str], exclude_kw: list[str]) -> bool:
    """Check if the article is about agriculture/food and not about excluded topics."""
    title_lower = title.lower()
    summary_lower = summary.lower()
    combined = title_lower + ' ' + summary_lower
    if any(kw in combined for kw in exclude_kw):
        return False
    # Keywords in title are a strong signal — 1 hit is enough
    title_hits = sum(1 for kw in include_kw if kw in title_lower)
    if title_hits >= 1:
        return True
    # Keywords only in summary — require 2+ to avoid false positives
    total_hits = sum(1 for kw in include_kw if kw in combined)
    return total_hits >= 2


def _fetch_rss_entries(max_age_days: int = 3) -> list[dict]:
    """Fetch and merge entries from all RSS feeds, filter by topic and age."""
    cutoff = datetime.now() - timedelta(days=max_age_days)
    entries = []

    feed_sources = _get_feed_sources()
    include_kw = _get_keywords('include')
    exclude_kw = _get_keywords('exclude')

    if not feed_sources:
        logger.warning('No active RSS sources in DB')
        return []
    if not include_kw:
        logger.info('No active include keywords in DB, using defaults')
        include_kw = _DEFAULT_INCLUDE_KW
    if not exclude_kw:
        exclude_kw = _DEFAULT_EXCLUDE_KW

    _rss_agent = 'Mozilla/5.0 (compatible; EdunaBazarBot/1.0)'
    for feed_info in feed_sources:
        try:
            feed = feedparser.parse(
                feed_info['url'],
                agent=_rss_agent,
                request_headers={'Accept': 'application/rss+xml, application/xml, text/xml'},
            )
            if feed.bozo and not feed.entries:
                logger.warning('RSS bozo error for %s: %s', feed_info['url'],
                               getattr(feed, 'bozo_exception', 'unknown'))
            logger.info('RSS %s: %d raw entries', feed_info['name'], len(feed.entries))
            for entry in feed.entries[:30]:
                title = _clean_html(getattr(entry, 'title', ''))
                summary = _clean_html(getattr(entry, 'summary', ''))
                link = getattr(entry, 'link', '')

                if not title or not link:
                    continue

                # Parse date
                pub_parsed = getattr(entry, 'published_parsed', None)
                if pub_parsed:
                    pub_dt = datetime(*pub_parsed[:6])
                else:
                    pub_dt = datetime.now()

                if pub_dt < cutoff:
                    continue

                if not _is_agro(title, summary, include_kw, exclude_kw):
                    continue

                entries.append({
                    'title': title,
                    'summary': summary[:1000],
                    'url': link,
                    'published': pub_dt,
                    'source': feed_info['name'],
                })
        except Exception as exc:
            logger.warning('RSS fetch error for %s: %s', feed_info['url'], exc)

    # Sort by date descending (newest first)
    entries.sort(key=lambda e: e['published'], reverse=True)
    return entries


def _get_gigachat_token() -> str:
    """Get or refresh GigaChat OAuth access token."""
    import time
    now = time.time()
    if _gigachat_token_cache['token'] and _gigachat_token_cache['expires'] > now + 60:
        return _gigachat_token_cache['token']

    auth_key = getattr(settings, 'GIGACHAT_AUTH_KEY', '')
    if not auth_key:
        return ''

    try:
        resp = requests.post(
            GIGACHAT_OAUTH_URL,
            headers={
                'Authorization': f'Basic {auth_key}',
                'RqUID': str(uuid.uuid4()),
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            data={'scope': GIGACHAT_SCOPE},
            verify=False,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data['access_token']
        # Token lives for 30 minutes (expires_at is in milliseconds)
        _gigachat_token_cache['token'] = token
        _gigachat_token_cache['expires'] = data.get('expires_at', 0) / 1000
        return token
    except Exception as exc:
        logger.error('GigaChat OAuth error: %s', exc)
        return ''


def _rewrite_with_gigachat(title: str, summary: str) -> dict | None:
    """Rewrite news title + text via GigaChat (Sber, free for individuals)."""
    auth_key = getattr(settings, 'GIGACHAT_AUTH_KEY', '')
    if not auth_key:
        logger.warning('GIGACHAT_AUTH_KEY not configured, skipping rewrite')
        return None

    token = _get_gigachat_token()
    if not token:
        return None

    prompt = f"""Перепиши следующую новость своими словами на русском языке.
Сделай уникальный заголовок (до 100 символов) и развёрнутый текст новости (5-7 предложений, 500-800 символов).
Не копируй текст дословно. Сохрани все факты, цифры и смысл. Пиши в информационном стиле, как для новостного портала.

Оригинальный заголовок: {title}
Оригинальный текст: {summary}

Ответь СТРОГО в формате:
ЗАГОЛОВОК: <новый заголовок>
ТЕКСТ: <новый текст>"""

    try:
        resp = requests.post(
            GIGACHAT_API_URL,
            headers={
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
            },
            json={
                'model': GIGACHAT_MODEL,
                'messages': [
                    {'role': 'system', 'content': 'Ты — копирайтер русскоязычного агро-портала. Пиши кратко и по делу.'},
                    {'role': 'user', 'content': prompt},
                ],
                'temperature': 0.7,
                'max_tokens': 1000,
            },
            verify=False,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        content = data['choices'][0]['message']['content'].strip()

        # Parse response
        new_title = ''
        new_text = ''
        collecting_text = False
        text_lines = []
        for line in content.split('\n'):
            stripped = line.strip()
            if stripped.upper().startswith('ЗАГОЛОВОК:'):
                new_title = stripped.split(':', 1)[1].strip()
                collecting_text = False
            elif stripped.upper().startswith('ТЕКСТ:'):
                text_lines = [stripped.split(':', 1)[1].strip()]
                collecting_text = True
            elif collecting_text and stripped:
                text_lines.append(stripped)
        new_text = ' '.join(text_lines).strip()

        if new_title and new_text:
            return {'title': new_title[:500], 'text': new_text[:1000]}

        # Fallback: use full response as text
        logger.warning('Could not parse GigaChat response, using raw content')
        lines = [l.strip() for l in content.split('\n') if l.strip()]
        if len(lines) >= 2:
            return {'title': lines[0][:500], 'text': ' '.join(lines[1:])[:1000]}

        return None

    except Exception as exc:
        logger.error('GigaChat API error: %s', exc)
        return None


def _check_relevance_gigachat(title: str, summary: str) -> bool:
    """Ask GigaChat whether the article is relevant to agriculture/food/agro-machinery."""
    token = _get_gigachat_token()
    if not token:
        return True  # Can't verify — let keyword filter decide

    prompt = f"""Определи, относится ли следующая новость к тематике:
- сельское хозяйство
- пищевая промышленность
- сельхозтехника и оборудование
- агропромышленный комплекс
- продовольствие и продукты питания

Заголовок: {title}
Текст: {summary[:500]}

Ответь ОДНИМ словом: ДА или НЕТ."""

    try:
        resp = requests.post(
            GIGACHAT_API_URL,
            headers={
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
            },
            json={
                'model': GIGACHAT_MODEL,
                'messages': [
                    {'role': 'user', 'content': prompt},
                ],
                'temperature': 0.1,
                'max_tokens': 10,
            },
            verify=False,
            timeout=15,
        )
        resp.raise_for_status()
        answer = resp.json()['choices'][0]['message']['content'].strip().lower()
        return answer.startswith('да')
    except Exception as exc:
        logger.warning('GigaChat relevance check error: %s', exc)
        return True  # On error, trust keyword filter


class Command(BaseCommand):
    help = 'Fetch and rewrite one agricultural news article per day'

    def add_arguments(self, parser):
        parser.add_argument('--dry', action='store_true', help='Preview without saving')
        parser.add_argument('--count', type=int, default=1, help='Number of articles to fetch')

    def handle(self, *args, **options):
        dry = options['dry']
        count = options['count']
        today = date.today()

        self.stdout.write(f'[fetch_news] Started at {timezone.now()}, today={today}, count={count}, dry={dry}')

        try:
            self._do_fetch(dry, count, today)
        except Exception as exc:
            self.stderr.write(self.style.ERROR(f'[fetch_news] FATAL ERROR: {exc}'))
            logger.exception('fetch_news crashed')
            raise

    def _do_fetch(self, dry, count, today):
        # Check how many news we already have for today
        existing_today = News.objects.filter(published_at=today).count()
        if existing_today >= count and not dry:
            self.stdout.write(self.style.WARNING(
                f'Already have {existing_today} news for {today}, skipping.'
            ))
            return

        remaining = count - existing_today if not dry else count
        self.stdout.write(f'[fetch_news] Need {remaining} more article(s) for today')

        # Check RSS sources
        from legacy.models import NewsFeedSource
        src_count = NewsFeedSource.objects.filter(is_active=True).count()
        self.stdout.write(f'[fetch_news] Active RSS sources in DB: {src_count}')
        if src_count == 0:
            self.stderr.write(self.style.ERROR(
                '[fetch_news] ERROR: No active RSS sources in news_feed_source table! '
                'Add sources via Django admin: /admin/legacy/newsfeedsource/'
            ))
            return

        # Check GigaChat config
        from django.conf import settings
        has_gigachat = bool(getattr(settings, 'GIGACHAT_AUTH_KEY', ''))
        self.stdout.write(f'[fetch_news] GigaChat configured: {has_gigachat}')

        entries = _fetch_rss_entries(max_age_days=3)
        self.stdout.write(f'[fetch_news] RSS entries found (after keyword filter): {len(entries)}')
        if not entries:
            self.stdout.write(self.style.WARNING('[fetch_news] No agro news found in RSS feeds.'))
            return

        # Filter out already saved URLs
        existing_urls = set(
            News.objects.filter(
                source_url__in=[e['url'] for e in entries]
            ).values_list('source_url', flat=True)
        )
        self.stdout.write(f'[fetch_news] Already saved URLs (skip): {len(existing_urls)}')

        saved = 0
        skipped_by_llm = []  # fallback candidates if GigaChat rejects all
        for entry in entries:
            if saved >= remaining:
                break

            if entry['url'] in existing_urls:
                continue

            self.stdout.write(f"\n--- Source: {entry['source']} ---")
            self.stdout.write(f"Original: {entry['title']}")

            # LLM relevance check
            if not _check_relevance_gigachat(entry['title'], entry['summary']):
                self.stdout.write(self.style.WARNING('  SKIP: not relevant (GigaChat)'))
                skipped_by_llm.append(entry)
                continue

            saved += self._save_entry(entry, dry, today)

        # Fallback: if GigaChat rejected ALL candidates, save best keyword matches
        if saved == 0 and skipped_by_llm:
            self.stdout.write(self.style.WARNING(
                f'\n[fetch_news] GigaChat rejected all {len(skipped_by_llm)} candidate(s). '
                f'Saving up to {remaining} by keyword score only.'
            ))
            for entry in skipped_by_llm[:remaining]:
                saved += self._save_entry(entry, dry, today)

        if saved > 0:
            invalidate_home_cache()
            self.stdout.write(self.style.SUCCESS(f'[fetch_news] Home cache invalidated'))
        self.stdout.write(self.style.SUCCESS(f'\n[fetch_news] Done. Saved {saved} article(s).'))

    def _save_entry(self, entry, dry, today):
        """Rewrite via GigaChat and save a single news entry. Returns 1 on success, 0 on failure."""
        rewritten = _rewrite_with_gigachat(entry['title'], entry['summary'])

        if rewritten:
            new_title = rewritten['title']
            new_text = rewritten['text']
            self.stdout.write(self.style.SUCCESS(f'Rewritten: {new_title}'))
        else:
            new_title = entry['title']
            new_text = entry['summary'][:300] if entry['summary'] else ''
            self.stdout.write(self.style.WARNING('Using original (no rewrite)'))

        if dry:
            self.stdout.write(f'Title: {new_title}')
            self.stdout.write(f'Text: {new_text}')
            self.stdout.write(f'URL: {entry["url"]}')
            return 1

        News.objects.create(
            title=new_title,
            text=new_text,
            source_url=entry['url'],
            source_name=entry['source'],
            source_title=entry['title'],
            published_at=today,
            is_active=True,
        )
        self.stdout.write(self.style.SUCCESS(f'Saved: {new_title}'))
        return 1
