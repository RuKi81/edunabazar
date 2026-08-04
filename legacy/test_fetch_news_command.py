"""
Тесты команды новостей (``legacy/management/commands/fetch_news.py``) —
страховочная сетка перед рефакторингом ``_fetch_rss_entries`` (C=13),
``_rewrite_with_gigachat`` (C=11) и ``Command._do_fetch`` (C=11).

Сеть замокана (feedparser / requests / GigaChat). Покрывается: фильтры
RSS (тематика/возраст/сортировка), парсинг ответа GigaChat с fallback,
оркестрация _do_fetch (лимит на день, дедуп URL, LLM-отсев с fallback,
--dry, инвалидация кеша главной).
"""
import io
import types
from datetime import datetime, timedelta
from unittest import mock

from django.core.management import call_command
from django.test import TestCase, override_settings

from .management.commands import fetch_news as fn
from .models import News, NewsFeedSource

_MOD = 'legacy.management.commands.fetch_news'


def _entry(title, summary='', link='https://ex.com/a', published=None):
    e = types.SimpleNamespace(title=title, summary=summary, link=link)
    published = published or datetime.now()
    e.published_parsed = published.timetuple()
    return e


def _feed(entries, bozo=False):
    return types.SimpleNamespace(entries=entries, bozo=bozo)


def _rss_item(url='https://ex.com/a', title='Урожай пшеницы вырос', source='Тест'):
    return {
        'title': title, 'summary': 'Подробности о зерне',
        'url': url, 'published': datetime.now(), 'source': source,
    }


class FetchRssEntriesTests(TestCase):

    def setUp(self):
        # data-миграция сидирует боевые RSS-источники — убираем их
        NewsFeedSource.objects.all().delete()
        NewsFeedSource.objects.create(name='Тест', url='https://ex.com/rss', is_active=True)

    def _fetch(self, feed):
        with mock.patch(f'{_MOD}.feedparser.parse', return_value=feed):
            return fn._fetch_rss_entries(max_age_days=3)

    def test_no_sources(self):
        NewsFeedSource.objects.all().delete()
        self.assertEqual(fn._fetch_rss_entries(), [])

    def test_title_keyword_hit_included(self):
        entries = self._fetch(_feed([_entry('Урожай пшеницы вырос')]))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]['source'], 'Тест')

    def test_offtopic_excluded(self):
        entries = self._fetch(_feed([_entry('Курс биткоина обновил максимум')]))
        self.assertEqual(entries, [])

    def test_summary_needs_two_hits(self):
        one_hit = _entry('Новости региона', summary='Обсуждали зерно', link='https://ex.com/1')
        two_hits = _entry('Новости региона', summary='Обсуждали зерно и урожай', link='https://ex.com/2')
        entries = self._fetch(_feed([one_hit, two_hits]))
        self.assertEqual([e['url'] for e in entries], ['https://ex.com/2'])

    def test_old_entries_cut_off(self):
        old = _entry('Урожай пшеницы', published=datetime.now() - timedelta(days=10))
        entries = self._fetch(_feed([old]))
        self.assertEqual(entries, [])

    def test_html_stripped_and_sorted_desc(self):
        older = _entry('<b>Урожай</b> пшеницы', link='https://ex.com/old',
                       published=datetime.now() - timedelta(days=1))
        newer = _entry('Комбайн новый', link='https://ex.com/new')
        entries = self._fetch(_feed([older, newer]))
        self.assertEqual([e['url'] for e in entries],
                         ['https://ex.com/new', 'https://ex.com/old'])
        self.assertEqual(entries[1]['title'], 'Урожай пшеницы')

    def test_feed_exception_swallowed(self):
        with mock.patch(f'{_MOD}.feedparser.parse', side_effect=OSError('boom')):
            self.assertEqual(fn._fetch_rss_entries(), [])


@override_settings(GIGACHAT_AUTH_KEY='key')
class RewriteGigachatTests(TestCase):

    def setUp(self):
        fn._gigachat_token_cache.update({'token': '', 'expires': 0})

    def _rewrite(self, content):
        resp = mock.Mock()
        resp.json.return_value = {'choices': [{'message': {'content': content}}]}
        with mock.patch(f'{_MOD}._get_gigachat_token', return_value='t'), \
                mock.patch(f'{_MOD}.requests.post', return_value=resp):
            return fn._rewrite_with_gigachat('Заголовок', 'Текст')

    @override_settings(GIGACHAT_AUTH_KEY='')
    def test_no_auth_key(self):
        self.assertIsNone(fn._rewrite_with_gigachat('t', 's'))

    def test_parses_structured_response(self):
        result = self._rewrite(
            'ЗАГОЛОВОК: Новый заголовок\nТЕКСТ: Первая строка.\nВторая строка.'
        )
        self.assertEqual(result['title'], 'Новый заголовок')
        self.assertEqual(result['text'], 'Первая строка. Вторая строка.')

    def test_fallback_to_raw_lines(self):
        result = self._rewrite('Просто заголовок\nПросто текст без маркеров.')
        self.assertEqual(result['title'], 'Просто заголовок')
        self.assertEqual(result['text'], 'Просто текст без маркеров.')

    def test_unparseable_single_line(self):
        self.assertIsNone(self._rewrite('Одна строка'))

    def test_api_error_returns_none(self):
        with mock.patch(f'{_MOD}._get_gigachat_token', return_value='t'), \
                mock.patch(f'{_MOD}.requests.post', side_effect=OSError('down')):
            self.assertIsNone(fn._rewrite_with_gigachat('t', 's'))


class DoFetchTests(TestCase):

    def setUp(self):
        NewsFeedSource.objects.all().delete()
        NewsFeedSource.objects.create(name='Тест', url='https://ex.com/rss', is_active=True)

    def _call(self, entries, relevant=True, rewritten=None, **options):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch(f'{_MOD}._fetch_rss_entries', return_value=entries), \
                mock.patch(f'{_MOD}._check_relevance_gigachat', return_value=relevant), \
                mock.patch(f'{_MOD}._rewrite_with_gigachat', return_value=rewritten), \
                mock.patch(f'{_MOD}.invalidate_home_cache') as inval:
            call_command('fetch_news', stdout=out, stderr=err, **options)
        return out.getvalue(), err.getvalue(), inval

    def test_saves_article_and_invalidates_cache(self):
        out, _, inval = self._call(
            [_rss_item()], rewritten={'title': 'Рерайт', 'text': 'Текст рерайта'},
        )
        news = News.objects.get()
        self.assertEqual(news.title, 'Рерайт')
        self.assertEqual(news.source_url, 'https://ex.com/a')
        self.assertEqual(news.source_title, 'Урожай пшеницы вырос')
        inval.assert_called_once()
        self.assertIn('Saved 1 article', out)

    def test_no_rewrite_uses_original(self):
        self._call([_rss_item()], rewritten=None)
        news = News.objects.get()
        self.assertEqual(news.title, 'Урожай пшеницы вырос')

    def test_dry_run_saves_nothing(self):
        _, _, inval = self._call(
            [_rss_item()], rewritten={'title': 'Р', 'text': 'Т'}, dry=True,
        )
        self.assertFalse(News.objects.exists())

    def test_skips_when_daily_quota_reached(self):
        News.objects.create(
            title='Есть', text='x', source_url='https://ex.com/z',
            published_at=datetime.now().date(), is_active=True,
        )
        out, _, _ = self._call([_rss_item()])
        self.assertIn('Already have', out)
        self.assertEqual(News.objects.count(), 1)

    def test_existing_url_skipped(self):
        News.objects.create(
            title='Есть', text='x', source_url='https://ex.com/a',
            published_at=datetime.now().date() - timedelta(days=1), is_active=True,
        )
        self._call([_rss_item(url='https://ex.com/a')])
        self.assertEqual(News.objects.count(), 1)

    def test_no_sources_error(self):
        NewsFeedSource.objects.all().delete()
        _, err, _ = self._call([_rss_item()])
        self.assertIn('No active RSS sources', err)
        self.assertFalse(News.objects.exists())

    def test_llm_reject_all_falls_back_to_keywords(self):
        out, _, _ = self._call([_rss_item()], relevant=False, rewritten=None)
        self.assertIn('rejected all', out)
        self.assertEqual(News.objects.count(), 1)
