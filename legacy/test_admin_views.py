"""
Тесты админских вьюх (``legacy/views/admin_views.py``) — страховочная
сетка перед рефакторингом ``admin_campaign_send_batch`` (C=21),
``admin_campaign_upload_excel`` (C=13) и ``admin_users_bulk_delete`` (C=12).

Покрывается: массовое удаление пользователей (права, парсинг id, защита
admin-аккаунтов, каскад, safe-next redirect), загрузка email-адресов из
Excel (валидация файла, извлечение адресов, дедупликация, счётчики),
батчевая отправка кампании (права, статусы кампании/логов, счётчики,
обработка SMTP-ошибок).
"""
import io
from unittest import mock

from django.contrib.gis.geos import Point
from django.core import mail
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.mail import EmailMultiAlternatives
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from .constants import USER_STATUS_ACTIVE
from .models import (
    Advert, Catalog, Categories, EmailCampaign, EmailLog, LegacyUser,
    Message, Review, Seller,
)

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}


def _make_user(username, **overrides):
    now = timezone.now()
    kwargs = dict(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RU', name='', address='',
        phone='', inn='', status=USER_STATUS_ACTIVE,
        created_at=now, updated_at=now, contacts='',
    )
    kwargs.update(overrides)
    return LegacyUser.objects.create(**kwargs)


def _login(client, user):
    client.get('/')
    session = client.session
    session['legacy_user_id'] = user.pk
    session.save()
    from django.conf import settings as _s
    client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key
    return client


def _admin_client():
    return _login(Client(), _make_user('admin'))


def _make_campaign(**overrides):
    kwargs = dict(
        name='Кампания', subject='Тема', body_html='<b>Привет</b>',
        body_text='Привет', from_email='noreply@test.com',
    )
    kwargs.update(overrides)
    return EmailCampaign.objects.create(**kwargs)


def _make_logs(campaign, emails, status=EmailLog.STATUS_PENDING):
    return [
        EmailLog.objects.create(campaign=campaign, recipient_email=e, status=status)
        for e in emails
    ]


def _xlsx_upload(rows, name='emails.xlsx'):
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return SimpleUploadedFile(name, buf.getvalue())


@override_settings(CACHES=_DUMMY_CACHE)
class BulkDeleteTests(TestCase):
    URL = '/legacy-admin/users/bulk-delete/'

    def setUp(self):
        self.client_ = _admin_client()

    def test_anonymous_redirected_to_login(self):
        resp = Client().post(self.URL, {'user_id': ['1']})
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(resp['Location'].startswith('/login/'))

    def test_non_admin_redirected(self):
        client = _login(Client(), _make_user('mortal'))
        resp = client.post(self.URL, {'user_id': ['1']})
        self.assertEqual(resp['Location'], '/adverts/')

    def test_get_redirects_without_deleting(self):
        victim = _make_user('victim')
        resp = self.client_.get(self.URL)
        self.assertEqual(resp['Location'], '/legacy-admin/')
        self.assertTrue(LegacyUser.objects.filter(pk=victim.pk).exists())

    def test_garbage_and_negative_ids_ignored(self):
        resp = self.client_.post(
            self.URL, {'user_id': ['abc', '-5', '0', '']},
        )
        self.assertEqual(resp['Location'], '/legacy-admin/')

    def test_unsafe_next_falls_back(self):
        victim = _make_user('victim')
        resp = self.client_.post(
            self.URL,
            {'user_id': [str(victim.pk)], 'next': 'https://evil.example/'},
        )
        self.assertEqual(resp['Location'], '/legacy-admin/')
        self.assertFalse(LegacyUser.objects.filter(pk=victim.pk).exists())

    def test_safe_next_respected(self):
        victim = _make_user('victim')
        resp = self.client_.post(
            self.URL,
            {'user_id': [str(victim.pk)], 'next': '/legacy-admin/?page=2'},
        )
        self.assertEqual(resp['Location'], '/legacy-admin/?page=2')

    def test_admin_account_protected(self):
        admin2 = LegacyUser.objects.get(username='admin')
        resp = self.client_.post(self.URL, {'user_id': [str(admin2.pk)]})
        self.assertEqual(resp['Location'], '/legacy-admin/')
        self.assertTrue(LegacyUser.objects.filter(pk=admin2.pk).exists())

    def test_cascade_deletes_related_objects(self):
        now = timezone.now()
        victim = _make_user('victim')
        other = _make_user('other')
        catalog = Catalog.objects.create(title='Зерно', sort=0, active=1)
        category = Categories.objects.create(
            catalog=catalog, title='Пшеница', active=1,
        )
        advert = Advert.objects.create(
            type=0, category=category, author=victim,
            location=Point(37.6, 55.7, srid=4326), contacts='+79001234567',
            title='Пшеница', text='Описание', price=100, price_unit='кг',
            wholesale_price=0, min_volume=0, wholesale_volume=0, volume=10,
            priority=0, created_at=now, updated_at=now, status=1,
            address='Москва',
        )
        Review.objects.create(
            type=0, object_id=advert.pk, points=5, author_id=victim.pk,
            text='Отзыв', created_at=now, updated_at=now, status=1,
        )
        Seller.objects.create(
            user=victim, name='Продавец', logo=0, location='', contacts={},
            price_list=0, links='', about='', created_at=now,
            updated_at=now, status=1,
        )
        Message.objects.create(
            sender=victim, recipient=other, text='привет', created_at=now,
        )
        Message.objects.create(
            sender=other, recipient=victim, text='ответ', created_at=now,
        )

        self.client_.post(self.URL, {'user_id': [str(victim.pk)]})

        self.assertFalse(LegacyUser.objects.filter(pk=victim.pk).exists())
        self.assertFalse(Advert.objects.filter(author_id=victim.pk).exists())
        self.assertFalse(Review.objects.filter(author_id=victim.pk).exists())
        self.assertFalse(Seller.objects.filter(user_id=victim.pk).exists())
        self.assertFalse(Message.objects.exists())
        self.assertTrue(LegacyUser.objects.filter(pk=other.pk).exists())


@override_settings(CACHES=_DUMMY_CACHE)
class CampaignUploadExcelTests(TestCase):

    def setUp(self):
        self.client_ = _admin_client()
        self.campaign = _make_campaign()
        self.url = f'/legacy-admin/campaigns/{self.campaign.pk}/upload-excel/'

    def _session_value(self, key):
        return self.client_.session.get(key, '')

    def test_get_redirects(self):
        resp = self.client_.get(self.url)
        self.assertEqual(
            resp['Location'], f'/legacy-admin/campaigns/{self.campaign.pk}/',
        )

    def test_no_file_sets_error(self):
        self.client_.post(self.url, {})
        self.assertEqual(self._session_value('campaign_upload_error'), 'Файл не выбран')

    def test_wrong_extension_rejected(self):
        f = SimpleUploadedFile('emails.csv', b'a@b.com')
        self.client_.post(self.url, {'excel_file': f})
        self.assertIn('.xlsx', self._session_value('campaign_upload_error'))

    def test_corrupt_file_reports_read_error(self):
        f = SimpleUploadedFile('emails.xlsx', b'not-a-workbook')
        self.client_.post(self.url, {'excel_file': f})
        self.assertIn(
            'Ошибка чтения файла', self._session_value('campaign_upload_error'),
        )
        self.assertFalse(EmailLog.objects.exists())

    def test_file_without_emails_reports_error(self):
        f = _xlsx_upload([['Иван', 'Петров'], ['без', 'адресов']])
        self.client_.post(self.url, {'excel_file': f})
        self.assertIn(
            'не найдено email', self._session_value('campaign_upload_error'),
        )

    def test_emails_extracted_normalized_and_counted(self):
        f = _xlsx_upload([
            ['  First@Test.COM  ', 'мусор'],
            ['контакт: second@test.com и third@test.com', None],
            ['first@test.com', 42],
        ])
        self.client_.post(self.url, {'excel_file': f})

        emails = set(
            EmailLog.objects.filter(campaign=self.campaign)
            .values_list('recipient_email', flat=True)
        )
        self.assertEqual(
            emails, {'first@test.com', 'second@test.com', 'third@test.com'},
        )
        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.total_recipients, 3)
        self.assertIn('Загружено 3', self._session_value('campaign_upload_message'))

    def test_existing_logs_deduplicated(self):
        _make_logs(self.campaign, ['old@test.com'])
        f = _xlsx_upload([['old@test.com'], ['new@test.com']])
        self.client_.post(self.url, {'excel_file': f})

        self.assertEqual(
            EmailLog.objects.filter(campaign=self.campaign).count(), 2,
        )
        msg = self._session_value('campaign_upload_message')
        self.assertIn('Загружено 1', msg)
        self.assertIn('Пропущено дублей: 1', msg)


def _failing_email_class(bad_emails):
    class _Failing(EmailMultiAlternatives):
        def send(self, fail_silently=False):
            if any(addr in bad_emails for addr in self.to):
                raise RuntimeError('SMTP 550 mailbox unavailable')
            return super().send(fail_silently)
    return _Failing


@override_settings(CACHES=_DUMMY_CACHE)
class CampaignSendBatchTests(TestCase):

    def setUp(self):
        self.client_ = _admin_client()
        self.campaign = _make_campaign()
        self.url = f'/legacy-admin/campaigns/{self.campaign.pk}/send-batch/'
        patcher = mock.patch('legacy.views.admin_views.time.sleep')
        patcher.start()
        self.addCleanup(patcher.stop)

    def _post_batch(self, logs):
        return self.client_.post(
            self.url, {'first_id': logs[0].pk, 'last_id': logs[-1].pk},
        )

    def test_anonymous_gets_403_json(self):
        resp = Client().post(self.url, {'first_id': 1, 'last_id': 2})
        self.assertEqual(resp.status_code, 403)
        self.assertFalse(resp.json()['ok'])

    def test_missing_ids_error(self):
        resp = self.client_.post(self.url, {})
        data = resp.json()
        self.assertFalse(data['ok'])
        self.assertEqual(data['error'], 'Missing batch IDs')

    def test_no_pending_logs_noop(self):
        logs = _make_logs(self.campaign, ['a@test.com'], status=EmailLog.STATUS_SENT)
        resp = self._post_batch(logs)
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertEqual(data['sent'], 0)
        self.assertEqual(len(mail.outbox), 0)
        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.status, EmailCampaign.STATUS_DRAFT)

    def test_successful_batch_marks_done(self):
        logs = _make_logs(self.campaign, ['a@test.com', 'b@test.com'])
        resp = self._post_batch(logs)
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertEqual(data['sent'], 2)
        self.assertEqual(data['failed'], 0)
        self.assertEqual(len(mail.outbox), 2)
        self.assertEqual(mail.outbox[0].from_email, 'noreply@test.com')
        self.assertEqual(mail.outbox[0].alternatives[0][1], 'text/html')

        for log in logs:
            log.refresh_from_db()
            self.assertEqual(log.status, EmailLog.STATUS_SENT)
            self.assertIsNotNone(log.sent_at)

        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.status, EmailCampaign.STATUS_DONE)
        self.assertEqual(self.campaign.sent_count, 2)
        self.assertEqual(self.campaign.failed_count, 0)
        self.assertIsNotNone(self.campaign.started_at)
        self.assertIsNotNone(self.campaign.finished_at)

    def test_partial_batch_keeps_sending_status(self):
        batch = _make_logs(self.campaign, ['a@test.com'])
        _make_logs(self.campaign, ['later@test.com'])  # вне диапазона батча
        self._post_batch(batch)
        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.status, EmailCampaign.STATUS_SENDING)
        self.assertIsNone(self.campaign.finished_at)

    def test_failed_email_logged_and_counted(self):
        logs = _make_logs(self.campaign, ['ok@test.com', 'bad@test.com'])
        with mock.patch(
            'legacy.views.admin_views.EmailMultiAlternatives',
            _failing_email_class({'bad@test.com'}),
        ):
            resp = self._post_batch(logs)
        data = resp.json()
        self.assertEqual(data['sent'], 1)
        self.assertEqual(data['failed'], 1)

        ok_log, bad_log = logs
        ok_log.refresh_from_db()
        bad_log.refresh_from_db()
        self.assertEqual(ok_log.status, EmailLog.STATUS_SENT)
        self.assertEqual(bad_log.status, EmailLog.STATUS_FAILED)
        self.assertIn('SMTP 550', bad_log.error_message)

        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.sent_count, 1)
        self.assertEqual(self.campaign.failed_count, 1)
        self.assertEqual(self.campaign.status, EmailCampaign.STATUS_DONE)

    def test_smtp_connect_error_returns_json_error(self):
        logs = _make_logs(self.campaign, ['a@test.com'])
        with mock.patch(
            'legacy.views.admin_views.get_connection',
            side_effect=OSError('connection refused'),
        ):
            resp = self._post_batch(logs)
        data = resp.json()
        self.assertFalse(data['ok'])
        self.assertIn('SMTP connect error', data['error'])
        logs[0].refresh_from_db()
        self.assertEqual(logs[0].status, EmailLog.STATUS_PENDING)

    def test_paused_campaign_resumes_to_sending(self):
        self.campaign.status = EmailCampaign.STATUS_PAUSED
        self.campaign.started_at = timezone.now()
        self.campaign.save(update_fields=['status', 'started_at'])
        started = self.campaign.started_at
        batch = _make_logs(self.campaign, ['a@test.com'])
        _make_logs(self.campaign, ['later@test.com'])
        self._post_batch(batch)
        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.status, EmailCampaign.STATUS_SENDING)
        self.assertEqual(self.campaign.started_at, started)
