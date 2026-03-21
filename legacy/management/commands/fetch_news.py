"""
Management command: fetch one agricultural news article per day,
rewrite it via GigaChat (Sber, free for individuals), and save to the News table.

Usage:
    python manage.py fetch_news          # fetch & rewrite 1 article
    python manage.py fetch_news --dry    # preview without saving
    python manage.py fetch_news --count 3  # fetch 3 articles

Cron (daily at 07:00 Moscow time):
    0 7 * * * cd /opt/edunabazar && docker compose -f deploy/app/docker-compose.yml exec -T web python manage.py fetch_news
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

from legacy.models import News

logger = logging.getLogger('legacy.fetch_news')

# ── RSS sources: agriculture & food, Russia + CIS ──────────────────────

RSS_FEEDS = [
    {
        'url': 'https://www.agroinvestor.ru/rss/',
        'name': 'Агроинвестор',
    },
    {
        'url': 'https://milknews.ru/rss.xml',
        'name': 'Milknews',
    },
    {
        'url': 'https://agrovesti.net/rss',
        'name': 'Агровести',
    },
    {
        'url': 'https://tass.ru/rss/v2.xml',
        'name': 'ТАСС',
    },
    {
        'url': 'https://rssexport.rbc.ru/rbcnews/news/30/full.rss',
        'name': 'РБК',
    },
    {
        'url': 'https://kazakh-zerno.net/rss/',
        'name': 'Казах-Зерно',
    },
]

# Keywords to filter for agriculture/food topics
AGRO_KEYWORDS = [
    'сельск', 'аграрн', 'фермер', 'урожай', 'зерн', 'пшениц',
    'кукуруз', 'подсолнеч', 'рапс', 'соя', 'ячмень', 'овёс', 'овес',
    'рожь', 'сахар', 'свёкл', 'свекл', 'картофел', 'овощ', 'фрукт',
    'молок', 'молоч', 'мяс', 'птиц', 'свин', 'говяд', 'баран',
    'рыб', 'аквакультур', 'удобрен', 'пестицид', 'гербицид',
    'комбайн', 'трактор', 'посев', 'уборк', 'уборочн',
    'агро', 'минсельхоз', 'россельхознадзор', 'продовольств',
    'экспорт зерн', 'импорт продовольств', 'животновод',
    'растениевод', 'садовод', 'тепличн', 'парник',
    'хлеб', 'мука', 'корм', 'комбикорм', 'элеватор', 'силос',
    'дойк', 'надо', 'стадо', 'поголовь', 'племен',
    'масло подсолн', 'масло растит', 'маргарин',
    'консерв', 'крупа', 'рис ', 'гречк', 'макарон',
    'колбас', 'сосиск', 'полуфабрикат', 'замороз',
    'кондитер', 'шоколад', 'конфет', 'печень',
    'напиток', 'сок', 'вод', 'пиво', 'вин',
    'чай ', 'кофе', 'какао',
    'орех', 'мёд', 'мед ', 'ягод', 'гриб',
    'специ', 'прянос', 'соус', 'кетчуп', 'майонез',
    'детское питан', 'продукт питан', 'пищев',
    'роспотребнадзор', 'качество продук',
]

# Negative keywords to exclude irrelevant topics
EXCLUDE_KEYWORDS = [
    'медицин', 'лекарств', 'вакцин', 'здоровь', 'больниц', 'клиник',
    'врач', 'пациент', 'диагноз', 'хирург', 'терапевт', 'онколог',
    'коронавирус', 'ковид', 'covid', 'грипп', 'эпидеми', 'пандеми',
    'госпитал', 'стоматолог', 'аптек', 'фармацевт', 'антибиотик',
    'криптовалют', 'биткоин', 'блокчейн',
    'футбол', 'хоккей', 'баскетбол', 'теннис', 'олимпи',
    'кинотеатр', 'сериал', 'актёр', 'актер', 'режиссёр', 'режиссер',
    'смартфон', 'iphone', 'android', 'гаджет',
    'космос', 'nasa', 'роскосмос', 'астроном',
    'военн', 'вооружен', 'артиллер', 'ракетн',
]

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


def _is_agro(title: str, summary: str) -> bool:
    """Check if the article is about agriculture/food and not about excluded topics."""
    combined = (title + ' ' + summary).lower()
    if any(kw in combined for kw in EXCLUDE_KEYWORDS):
        return False
    return any(kw in combined for kw in AGRO_KEYWORDS)


def _fetch_rss_entries(max_age_days: int = 3) -> list[dict]:
    """Fetch and merge entries from all RSS feeds, filter by topic and age."""
    cutoff = datetime.now() - timedelta(days=max_age_days)
    entries = []

    for feed_info in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_info['url'])
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

                if not _is_agro(title, summary):
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
        for line in content.split('\n'):
            line = line.strip()
            if line.upper().startswith('ЗАГОЛОВОК:'):
                new_title = line.split(':', 1)[1].strip()
            elif line.upper().startswith('ТЕКСТ:'):
                new_text = line.split(':', 1)[1].strip()

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


class Command(BaseCommand):
    help = 'Fetch and rewrite one agricultural news article per day'

    def add_arguments(self, parser):
        parser.add_argument('--dry', action='store_true', help='Preview without saving')
        parser.add_argument('--count', type=int, default=1, help='Number of articles to fetch')

    def handle(self, *args, **options):
        dry = options['dry']
        count = options['count']
        today = date.today()

        # Check how many news we already have for today
        existing_today = News.objects.filter(published_at=today).count()
        if existing_today >= count and not dry:
            self.stdout.write(self.style.WARNING(
                f'Already have {existing_today} news for {today}, skipping.'
            ))
            return

        remaining = count - existing_today if not dry else count

        entries = _fetch_rss_entries(max_age_days=3)
        if not entries:
            self.stdout.write(self.style.WARNING('No agro news found in RSS feeds.'))
            return

        # Filter out already saved URLs
        existing_urls = set(
            News.objects.filter(
                source_url__in=[e['url'] for e in entries]
            ).values_list('source_url', flat=True)
        )

        saved = 0
        for entry in entries:
            if saved >= remaining:
                break

            if entry['url'] in existing_urls:
                continue

            self.stdout.write(f"\n--- Source: {entry['source']} ---")
            self.stdout.write(f"Original: {entry['title']}")

            # Try to rewrite via LLM
            rewritten = _rewrite_with_gigachat(entry['title'], entry['summary'])

            if rewritten:
                new_title = rewritten['title']
                new_text = rewritten['text']
                self.stdout.write(self.style.SUCCESS(f'Rewritten: {new_title}'))
            else:
                # Fallback: use original title with trimmed summary
                new_title = entry['title']
                new_text = entry['summary'][:300] if entry['summary'] else ''
                self.stdout.write(self.style.WARNING('Using original (no rewrite)'))

            if dry:
                self.stdout.write(f'Title: {new_title}')
                self.stdout.write(f'Text: {new_text}')
                self.stdout.write(f'URL: {entry["url"]}')
                saved += 1
                continue

            News.objects.create(
                title=new_title,
                text=new_text,
                source_url=entry['url'],
                source_name=entry['source'],
                source_title=entry['title'],
                published_at=today,
                is_active=True,
            )
            saved += 1
            self.stdout.write(self.style.SUCCESS(f'Saved: {new_title}'))

        self.stdout.write(self.style.SUCCESS(f'\nDone. Saved {saved} article(s).'))
