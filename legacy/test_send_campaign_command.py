"""
Тесты команды рассылки (``legacy/management/commands/send_campaign.py``) —
страховочная сетка перед рефакторингом ``_send_campaign`` (C=40).

Покрывается: guard-статусы (done/sending/paused без --resume), заполнение
логов по аудитории (дедуп, lowercase), пропуск отписавшихся, успешная
отправка (подстановка unsubscribe-url, заголовок List-Unsubscribe,
счётчики, статус done), --dry-run, --limit, ретраи с восстановлением,
окончательный fail после исчерпания ретраев, автопауза после серии сбоев.
"""
import io
from unittest import mock

from django.core import mail
from django.core.mail import EmailMultiAlternatives
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, override_settings
from django.utils import timezone

from .models import EmailCampaign, EmailLog, EmailUnsubscribe, LegacyUser

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}

_MOD = 'legacy.management.commands.send_campaign'


def _make_user(username, email, status=10):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=email, currency='RU', name='', address='',
        phone='', inn='', status=status,
        created_at=now, updated_at=now, contacts='',
    )


def _make_campaign(**overrides):
    kwargs = dict(
        name='Кампания', subject='Тема',
        body_html='<a href="{{ unsubscribe_url }}">Отписаться</a>',
        body_text='Текст. Отписка: {{ unsubscribe_url }}',
        from_email='noreply@test.com',
    )
    kwargs.update(overrides)
    return EmailCampaign.objects.create(**kwargs)


def _make_logs(campaign, emails, status=EmailLog.STATUS_PENDING):
    return [
        EmailLog.objects.create(campaign=campaign, recipient_email=e, status=status)
        for e in emails
    ]


def _call(campaign, **options):
    out, err = io.StringIO(), io.StringIO()
    with mock.patch(f'{_MOD}.time.sleep'):
        call_command('send_campaign', campaign.pk, stdout=out, stderr=err, **options)
    return out.getvalue(), err.getvalue()


class _FailingEmail(EmailMultiAlternatives):
    """Всегда падает для адресов из ``bad``; ``flaky`` падают один раз."""
    bad: set = set()
    flaky_budget: dict = {}

    def send(self, fail_silently=False):
        to = self.to[0]
        if to in self.bad:
            raise RuntimeError('SMTP 554 permanent failure')
        if self.flaky_budget.get(to, 0) > 0:
            self.flaky_budget[to] -= 1
            raise RuntimeError('SMTP 421 transient failure')
        return super().send(fail_silently)


@override_settings(CACHES=_DUMMY_CACHE)
class SendCampaignGuardTests(TestCase):

    def test_unknown_campaign(self):
        with self.assertRaisesMessage(CommandError, 'not found'):
            call_command('send_campaign', 999999)

    def test_done_campaign_rejected(self):
        c = _make_campaign(status=EmailCampaign.STATUS_DONE)
        with self.assertRaisesMessage(CommandError, 'already done'):
            call_command('send_campaign', c.pk)

    def test_sending_without_resume_rejected(self):
        c = _make_campaign(status=EmailCampaign.STATUS_SENDING)
        with self.assertRaisesMessage(CommandError, 'already sending'):
            call_command('send_campaign', c.pk)

    def test_paused_without_resume_rejected(self):
        c = _make_campaign(status=EmailCampaign.STATUS_PAUSED)
        with self.assertRaisesMessage(CommandError, 'is paused'):
            call_command('send_campaign', c.pk)


@override_settings(CACHES=_DUMMY_CACHE)
class SendCampaignPopulateTests(TestCase):

    def test_populates_from_audience_with_dedup(self):
        _make_user('u1', 'One@Test.com')
        _make_user('u2', 'one@test.com')      # дубль после lowercase
        _make_user('u3', 'two@test.com')
        _make_user('imp', 'imported@test.com', status=0)
        c = _make_campaign(audience=EmailCampaign.AUDIENCE_REGISTERED)

        _call(c, dry_run=True)

        emails = sorted(
            EmailLog.objects.filter(campaign=c).values_list('recipient_email', flat=True)
        )
        self.assertEqual(emails, ['one@test.com', 'two@test.com'])
        c.refresh_from_db()
        self.assertEqual(c.total_recipients, 2)

    def test_imported_audience_excludes_active(self):
        _make_user('u1', 'active@test.com')
        _make_user('imp', 'imported@test.com', status=0)
        c = _make_campaign(audience=EmailCampaign.AUDIENCE_IMPORTED)

        _call(c, dry_run=True)

        emails = list(
            EmailLog.objects.filter(campaign=c).values_list('recipient_email', flat=True)
        )
        self.assertEqual(emails, ['imported@test.com'])

    def test_prepopulated_logs_reused(self):
        _make_user('u1', 'user@test.com')
        c = _make_campaign()
        _make_logs(c, ['manual@test.com'])
        c.total_recipients = 1
        c.save(update_fields=['total_recipients'])

        out, _ = _call(c, dry_run=True)

        self.assertIn('pre-populated', out)
        emails = list(
            EmailLog.objects.filter(campaign=c).values_list('recipient_email', flat=True)
        )
        self.assertEqual(emails, ['manual@test.com'])


@override_settings(CACHES=_DUMMY_CACHE)
class SendCampaignSendTests(TestCase):

    def setUp(self):
        self.campaign = _make_campaign()
        _FailingEmail.bad = set()
        _FailingEmail.flaky_budget = {}

    def test_successful_send(self):
        logs = _make_logs(self.campaign, ['a@test.com', 'b@test.com'])
        _call(self.campaign)

        self.assertEqual(len(mail.outbox), 2)
        msg = mail.outbox[0]
        self.assertEqual(msg.from_email, 'noreply@test.com')
        self.assertIn('/unsubscribe/', msg.body)
        self.assertNotIn('{{ unsubscribe_url }}', msg.body)
        self.assertIn('/unsubscribe/', msg.alternatives[0][0])
        self.assertIn('List-Unsubscribe', msg.extra_headers)

        for log in logs:
            log.refresh_from_db()
            self.assertEqual(log.status, EmailLog.STATUS_SENT)

        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.status, EmailCampaign.STATUS_DONE)
        self.assertEqual(self.campaign.sent_count, 2)
        self.assertIsNotNone(self.campaign.finished_at)

    def test_dry_run_marks_sent_without_sending(self):
        logs = _make_logs(self.campaign, ['a@test.com'])
        _call(self.campaign, dry_run=True)

        self.assertEqual(len(mail.outbox), 0)
        logs[0].refresh_from_db()
        self.assertEqual(logs[0].status, EmailLog.STATUS_SENT)
        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.status, EmailCampaign.STATUS_DONE)

    def test_limit_stops_run(self):
        _make_logs(self.campaign, ['a@test.com', 'b@test.com', 'c@test.com'])
        _call(self.campaign, limit=2)

        self.assertEqual(len(mail.outbox), 2)
        self.campaign.refresh_from_db()
        # остался pending — кампания не done
        self.assertEqual(self.campaign.status, EmailCampaign.STATUS_SENDING)
        self.assertEqual(
            EmailLog.objects.filter(
                campaign=self.campaign, status=EmailLog.STATUS_PENDING,
            ).count(),
            1,
        )

    def test_unsubscribed_skipped(self):
        EmailUnsubscribe.objects.create(email='unsub@test.com')
        logs = _make_logs(self.campaign, ['unsub@test.com', 'ok@test.com'])
        out, _ = _call(self.campaign)

        self.assertIn('Skipped 1 unsubscribed', out)
        self.assertEqual(len(mail.outbox), 1)
        unsub_log, ok_log = logs
        unsub_log.refresh_from_db()
        self.assertEqual(unsub_log.status, EmailLog.STATUS_FAILED)
        self.assertEqual(unsub_log.error_message, 'unsubscribed')
        ok_log.refresh_from_db()
        self.assertEqual(ok_log.status, EmailLog.STATUS_SENT)

    def test_transient_failure_retried(self):
        logs = _make_logs(self.campaign, ['flaky@test.com'])
        _FailingEmail.flaky_budget = {'flaky@test.com': 1}
        with mock.patch(f'{_MOD}.EmailMultiAlternatives', _FailingEmail):
            _call(self.campaign)

        logs[0].refresh_from_db()
        self.assertEqual(logs[0].status, EmailLog.STATUS_SENT)
        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.sent_count, 1)
        self.assertEqual(self.campaign.failed_count, 0)

    def test_permanent_failure_marked_after_retries(self):
        logs = _make_logs(self.campaign, ['bad@test.com', 'ok@test.com'])
        _FailingEmail.bad = {'bad@test.com'}
        with mock.patch(f'{_MOD}.EmailMultiAlternatives', _FailingEmail):
            _call(self.campaign)

        bad_log, ok_log = logs
        bad_log.refresh_from_db()
        self.assertEqual(bad_log.status, EmailLog.STATUS_FAILED)
        self.assertIn('SMTP 554', bad_log.error_message)
        ok_log.refresh_from_db()
        self.assertEqual(ok_log.status, EmailLog.STATUS_SENT)

        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.status, EmailCampaign.STATUS_DONE)
        self.assertEqual(self.campaign.sent_count, 1)
        self.assertEqual(self.campaign.failed_count, 1)

    def test_pauses_after_max_consecutive_failures(self):
        emails = [f'bad{i}@test.com' for i in range(11)]
        _make_logs(self.campaign, emails)
        _FailingEmail.bad = set(emails)
        with mock.patch(f'{_MOD}.EmailMultiAlternatives', _FailingEmail):
            _, err = _call(self.campaign)

        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.status, EmailCampaign.STATUS_PAUSED)
        self.assertEqual(self.campaign.failed_count, 10)
        # 11-й получатель не тронут
        self.assertEqual(
            EmailLog.objects.filter(
                campaign=self.campaign, status=EmailLog.STATUS_PENDING,
            ).count(),
            1,
        )
        self.assertIn('consecutive failures', err)
