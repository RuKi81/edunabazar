"""
Тесты команды импорта адресов (``legacy/management/commands/
import_campaign_emails.py``) — страховочная сетка перед рефакторингом
``handle`` (C=19).

Покрывается: чтение колонки A (пропуск заголовка), валидация и дедуп,
исключение получателей прошлых кампаний (--exclude-campaign / --exclude-all)
и отписавшихся, --dry-run, ошибки файлов, создание кампании с логами.
"""
import io
import tempfile
from pathlib import Path

import openpyxl
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from .models import EmailCampaign, EmailLog, EmailUnsubscribe


def _write_xlsx(path, rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(['Email'])  # header
    for row in rows:
        ws.append([row])
    wb.save(path)


class ImportCampaignEmailsTests(TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.xlsx = str(Path(self.tmp.name) / 'emails.xlsx')
        self.body_html = str(Path(self.tmp.name) / 'body.html')
        Path(self.body_html).write_text('<b>Привет</b>', encoding='utf-8')

    def _call(self, *args, **options):
        out = io.StringIO()
        base = dict(
            name='Импорт', subject='Тема', body_html_file=self.body_html,
        )
        base.update(options)
        call_command('import_campaign_emails', self.xlsx, *args, stdout=out, **base)
        return out.getvalue()

    def test_import_validate_dedup_lowercase(self):
        _write_xlsx(self.xlsx, [
            ' First@Test.COM ', 'first@test.com', 'not-an-email',
            'second@test.com', None, 42,
        ])
        out = self._call()

        campaign = EmailCampaign.objects.get()
        self.assertEqual(campaign.audience, EmailCampaign.AUDIENCE_IMPORTED)
        self.assertEqual(campaign.status, EmailCampaign.STATUS_DRAFT)
        self.assertEqual(campaign.total_recipients, 2)
        self.assertEqual(campaign.body_html, '<b>Привет</b>')

        emails = sorted(
            EmailLog.objects.filter(campaign=campaign)
            .values_list('recipient_email', flat=True)
        )
        self.assertEqual(emails, ['first@test.com', 'second@test.com'])
        self.assertIn('invalid: 1', out)

    def test_body_text_file(self):
        body_text = str(Path(self.tmp.name) / 'body.txt')
        Path(body_text).write_text('Просто текст', encoding='utf-8')
        _write_xlsx(self.xlsx, ['a@test.com'])
        self._call(body_text_file=body_text)
        self.assertEqual(EmailCampaign.objects.get().body_text, 'Просто текст')

    def test_exclude_campaign(self):
        old = EmailCampaign.objects.create(name='Старая', subject='s', body_html='x')
        EmailLog.objects.create(campaign=old, recipient_email='old@test.com')
        _write_xlsx(self.xlsx, ['old@test.com', 'new@test.com'])

        self._call(exclude_campaigns=[old.pk])

        new_campaign = EmailCampaign.objects.exclude(pk=old.pk).get()
        emails = list(
            EmailLog.objects.filter(campaign=new_campaign)
            .values_list('recipient_email', flat=True)
        )
        self.assertEqual(emails, ['new@test.com'])

    def test_exclude_all(self):
        old1 = EmailCampaign.objects.create(name='C1', subject='s', body_html='x')
        old2 = EmailCampaign.objects.create(name='C2', subject='s', body_html='x')
        EmailLog.objects.create(campaign=old1, recipient_email='a@test.com')
        EmailLog.objects.create(campaign=old2, recipient_email='b@test.com')
        _write_xlsx(self.xlsx, ['a@test.com', 'b@test.com', 'c@test.com'])

        self._call(exclude_all=True)

        new_campaign = EmailCampaign.objects.exclude(pk__in=[old1.pk, old2.pk]).get()
        emails = list(
            EmailLog.objects.filter(campaign=new_campaign)
            .values_list('recipient_email', flat=True)
        )
        self.assertEqual(emails, ['c@test.com'])

    def test_unsubscribed_excluded(self):
        EmailUnsubscribe.objects.create(email='unsub@test.com')
        _write_xlsx(self.xlsx, ['unsub@test.com', 'ok@test.com'])

        self._call()

        emails = list(EmailLog.objects.values_list('recipient_email', flat=True))
        self.assertEqual(emails, ['ok@test.com'])

    def test_dry_run_creates_nothing(self):
        _write_xlsx(self.xlsx, ['a@test.com'])
        out = self._call(dry_run=True)
        self.assertIn('DRY RUN', out)
        self.assertFalse(EmailCampaign.objects.exists())
        self.assertFalse(EmailLog.objects.exists())

    def test_no_emails_after_exclusion(self):
        EmailUnsubscribe.objects.create(email='unsub@test.com')
        _write_xlsx(self.xlsx, ['unsub@test.com'])
        with self.assertRaisesMessage(CommandError, 'No emails to send'):
            self._call()

    def test_bad_xlsx_path(self):
        with self.assertRaisesMessage(CommandError, 'Cannot open Excel file'):
            call_command(
                'import_campaign_emails', str(Path(self.tmp.name) / 'nope.xlsx'),
                name='x', subject='s', body_html_file=self.body_html,
            )

    def test_missing_html_body(self):
        _write_xlsx(self.xlsx, ['a@test.com'])
        with self.assertRaisesMessage(CommandError, 'Cannot read HTML body file'):
            self._call(body_html_file=str(Path(self.tmp.name) / 'nope.html'))
