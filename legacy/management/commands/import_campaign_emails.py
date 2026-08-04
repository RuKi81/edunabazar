"""
Import emails from an Excel file into a new EmailCampaign,
excluding addresses that were already used in previous campaigns.

Usage:
    python manage.py import_campaign_emails <xlsx_path> --name "Campaign name" \
        --subject "Email subject" --body-html body.html [--exclude-campaign <id>...]

The Excel file should have emails in column A (first row = header).
"""

import re

import openpyxl
from django.core.management.base import BaseCommand, CommandError

from legacy.models import EmailCampaign, EmailLog, EmailUnsubscribe


class Command(BaseCommand):
    help = 'Import emails from Excel into a new campaign, excluding previous recipients'

    def add_arguments(self, parser):
        parser.add_argument('xlsx', help='Path to .xlsx file with emails in column A')
        parser.add_argument('--name', required=True, help='Campaign name')
        parser.add_argument('--subject', required=True, help='Email subject line')
        parser.add_argument('--body-html', required=True, dest='body_html_file',
                            help='Path to HTML file with email body')
        parser.add_argument('--body-text', dest='body_text_file', default='',
                            help='Path to plain-text file with email body (optional)')
        parser.add_argument('--from-email', dest='from_email', default='',
                            help='Sender address (optional, uses DEFAULT_FROM_EMAIL)')
        parser.add_argument('--exclude-campaign', dest='exclude_campaigns',
                            type=int, nargs='*', default=[],
                            help='IDs of campaigns whose recipients to exclude')
        parser.add_argument('--exclude-all', dest='exclude_all', action='store_true',
                            help='Exclude recipients from ALL previous campaigns')
        parser.add_argument('--dry-run', action='store_true',
                            help='Show counts but do not create anything')

    def handle(self, *args, **options):
        raw_emails = self._read_xlsx_emails(options['xlsx'])
        valid_emails = self._validate_and_dedup(raw_emails)

        exclude_set = self._build_exclusion_set(
            options['exclude_all'], options['exclude_campaigns'],
        )
        final_emails = [e for e in valid_emails if e not in exclude_set]
        excluded_count = len(valid_emails) - len(final_emails)
        self.stdout.write(f'  After exclusion: {len(final_emails)} (excluded {excluded_count})')

        if options['dry_run']:
            self.stdout.write(self.style.WARNING('DRY RUN — nothing created'))
            return

        if not final_emails:
            raise CommandError('No emails to send after exclusion')

        body_html = self._read_body_file(options['body_html_file'], 'HTML')
        body_text = ''
        if options['body_text_file']:
            body_text = self._read_body_file(options['body_text_file'], 'text')

        self._create_campaign(options, body_html, body_text, final_emails)

    def _read_xlsx_emails(self, xlsx_path):
        """Колонка A без заголовка → список lowercase-строк."""
        self.stdout.write(f'Reading {xlsx_path}...')
        try:
            wb = openpyxl.load_workbook(xlsx_path, read_only=True)
            ws = wb.active
        except Exception as e:
            raise CommandError(f'Cannot open Excel file: {e}')

        raw_emails = []
        for i, row in enumerate(ws.iter_rows(min_col=1, max_col=1, values_only=True)):
            if i == 0:  # skip header
                continue
            val = row[0]
            if val and isinstance(val, str):
                raw_emails.append(val.strip().lower())
        wb.close()

        self.stdout.write(f'  Raw rows: {len(raw_emails)}')
        return raw_emails

    def _validate_and_dedup(self, raw_emails):
        """Уникальные адреса, прошедшие regex-валидацию (порядок сохраняется)."""
        email_re = re.compile(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')
        seen = set()
        valid_emails = []
        invalid_count = 0
        for e in raw_emails:
            if not e or e in seen:
                continue
            seen.add(e)
            if email_re.match(e):
                valid_emails.append(e)
            else:
                invalid_count += 1

        self.stdout.write(f'  Unique valid: {len(valid_emails)}, invalid: {invalid_count}')
        return valid_emails

    def _build_exclusion_set(self, exclude_all, exclude_ids):
        """Получатели прошлых кампаний (всех или выбранных) + отписавшиеся."""
        exclude_set = set()
        if exclude_all:
            exclude_set = set(
                EmailLog.objects.values_list('recipient_email', flat=True)
                .distinct()
            )
            self.stdout.write(
                f'  Excluding ALL previous recipients: {len(exclude_set)} addresses'
            )
        elif exclude_ids:
            exclude_set = set(
                EmailLog.objects.filter(campaign_id__in=exclude_ids)
                .values_list('recipient_email', flat=True)
                .distinct()
            )
            self.stdout.write(
                f'  Excluding from campaigns {exclude_ids}: {len(exclude_set)} addresses'
            )

        # Normalize exclusion set
        exclude_set = {e.strip().lower() for e in exclude_set if e}

        # Also exclude unsubscribed emails
        unsub_set = set(
            EmailUnsubscribe.objects.values_list('email', flat=True)
        )
        unsub_set = {e.strip().lower() for e in unsub_set if e}
        if unsub_set:
            self.stdout.write(f'  Excluding {len(unsub_set)} unsubscribed addresses')
        return exclude_set | unsub_set

    def _read_body_file(self, path, label):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            raise CommandError(f'Cannot read {label} body file: {e}')

    def _create_campaign(self, options, body_html, body_text, final_emails):
        campaign = EmailCampaign.objects.create(
            name=options['name'],
            subject=options['subject'],
            body_html=body_html,
            body_text=body_text,
            from_email=options['from_email'],
            audience=EmailCampaign.AUDIENCE_IMPORTED,
            status=EmailCampaign.STATUS_DRAFT,
            total_recipients=len(final_emails),
        )

        self.stdout.write(f'  Created campaign #{campaign.pk}: {campaign.name}')

        logs = [
            EmailLog(campaign=campaign, recipient_email=email)
            for email in final_emails
        ]
        EmailLog.objects.bulk_create(logs, batch_size=1000)

        self.stdout.write(self.style.SUCCESS(
            f'  ✅ Campaign #{campaign.pk} ready with {len(final_emails)} recipients\n'
            f'  To send: python manage.py send_campaign {campaign.pk}'
        ))
