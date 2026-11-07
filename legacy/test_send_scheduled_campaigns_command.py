"""Тесты команды ``send_scheduled_campaigns`` (ежедневная автоотправка групп)."""
import datetime
import io
from unittest import mock

from django.core import mail
from django.core.management import call_command
from django.test import TestCase, override_settings
from django.utils import timezone

from .campaign_batches import build_batches, next_pending_batch
from .models import EmailCampaign, EmailLog

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}

MSK = datetime.timezone(datetime.timedelta(hours=3))


def _make_campaign(**overrides):
    kwargs = dict(
        name='Кампания', subject='Тема', body_html='<b>Привет</b>',
        body_text='Привет', from_email='noreply@test.com',
        auto_send_enabled=True, auto_send_time=datetime.time(9, 30),
    )
    kwargs.update(overrides)
    return EmailCampaign.objects.create(**kwargs)


def _make_logs(campaign, emails, status=EmailLog.STATUS_PENDING):
    return [
        EmailLog.objects.create(campaign=campaign, recipient_email=e, status=status)
        for e in emails
    ]


def _run(now_local):
    out = io.StringIO()
    with mock.patch('legacy.management.commands.send_scheduled_campaigns.timezone.localtime',
                    return_value=now_local):
        call_command('send_scheduled_campaigns', stdout=out, stderr=out)
    return out.getvalue()


TODAY = datetime.date(2026, 11, 7)
AFTER = datetime.datetime(2026, 11, 7, 9, 35, tzinfo=MSK)
BEFORE = datetime.datetime(2026, 11, 7, 9, 0, tzinfo=MSK)


class BatchHelpersTests(TestCase):

    def test_next_pending_batch_skips_done(self):
        c = _make_campaign()
        _make_logs(c, ['a@test.com', 'b@test.com'], status=EmailLog.STATUS_SENT)
        _make_logs(c, ['c@test.com'])
        with mock.patch('legacy.campaign_batches.BATCH_SIZE', 2):
            batches = build_batches(c, batch_size=2)
            nxt = next_pending_batch(c, batch_size=2)
        self.assertEqual(len(batches), 2)
        self.assertTrue(batches[0]['done'])
        self.assertEqual(nxt['num'], 2)

    def test_next_pending_batch_none_when_all_done(self):
        c = _make_campaign()
        _make_logs(c, ['a@test.com'], status=EmailLog.STATUS_FAILED)
        self.assertIsNone(next_pending_batch(c))


@override_settings(CACHES=_DUMMY_CACHE)
class SendScheduledCampaignsTests(TestCase):

    def setUp(self):
        patcher = mock.patch('legacy.views.admin_views.time.sleep')
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_sends_first_pending_batch_after_time(self):
        c = _make_campaign()
        _make_logs(c, ['a@test.com', 'b@test.com'])
        _run(AFTER)
        self.assertEqual(len(mail.outbox), 2)
        c.refresh_from_db()
        self.assertEqual(c.auto_send_last_date, TODAY)
        self.assertEqual(c.sent_count, 2)

    def test_not_sent_before_time(self):
        c = _make_campaign()
        _make_logs(c, ['a@test.com'])
        _run(BEFORE)
        self.assertEqual(len(mail.outbox), 0)
        c.refresh_from_db()
        self.assertIsNone(c.auto_send_last_date)
        self.assertTrue(c.auto_send_enabled)

    def test_disabled_campaign_ignored(self):
        c = _make_campaign(auto_send_enabled=False)
        _make_logs(c, ['a@test.com'])
        _run(AFTER)
        self.assertEqual(len(mail.outbox), 0)

    def test_only_one_batch_per_day(self):
        c = _make_campaign()
        _make_logs(c, ['a@test.com', 'b@test.com', 'c@test.com'])
        with mock.patch('legacy.campaign_batches.BATCH_SIZE', 2):
            _run(AFTER)
            _run(datetime.datetime(2026, 11, 7, 18, 0, tzinfo=MSK))
        self.assertEqual(len(mail.outbox), 2)
        c.refresh_from_db()
        self.assertTrue(c.auto_send_enabled)
        self.assertEqual(EmailLog.objects.filter(campaign=c, status=EmailLog.STATUS_PENDING).count(), 1)

    def test_next_day_sends_next_batch_in_order(self):
        c = _make_campaign()
        logs = _make_logs(c, ['a@test.com', 'b@test.com', 'c@test.com'])
        with mock.patch('legacy.campaign_batches.BATCH_SIZE', 2):
            _run(AFTER)
            _run(datetime.datetime(2026, 11, 8, 9, 31, tzinfo=MSK))
        self.assertEqual(len(mail.outbox), 3)
        self.assertEqual(mail.outbox[-1].to, [logs[2].recipient_email])
        c.refresh_from_db()
        self.assertEqual(c.auto_send_last_date, datetime.date(2026, 11, 8))

    def test_checkbox_cleared_when_last_batch_sent(self):
        c = _make_campaign()
        _make_logs(c, ['a@test.com'])
        out = _run(AFTER)
        c.refresh_from_db()
        self.assertFalse(c.auto_send_enabled)
        self.assertEqual(c.status, EmailCampaign.STATUS_DONE)
        self.assertIn('all batches sent', out)

    def test_checkbox_cleared_when_nothing_pending(self):
        c = _make_campaign()
        _make_logs(c, ['a@test.com'], status=EmailLog.STATUS_SENT)
        out = _run(AFTER)
        self.assertEqual(len(mail.outbox), 0)
        c.refresh_from_db()
        self.assertFalse(c.auto_send_enabled)
        self.assertIn('no pending batches', out)

    def test_smtp_connect_error_keeps_logs_pending(self):
        c = _make_campaign()
        _make_logs(c, ['a@test.com'])
        with mock.patch('legacy.views.admin_views.get_connection',
                        side_effect=OSError('connection refused')):
            out = _run(AFTER)
        self.assertIn('SMTP connect error', out)
        self.assertEqual(
            EmailLog.objects.filter(campaign=c, status=EmailLog.STATUS_PENDING).count(), 1,
        )
        c.refresh_from_db()
        self.assertTrue(c.auto_send_enabled)

    def test_uses_moscow_local_time(self):
        self.assertEqual(timezone.get_current_timezone_name(), 'Europe/Moscow')
