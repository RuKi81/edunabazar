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

from legacy.models import EmailCampaign, EmailLog


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
        xlsx_path = options['xlsx']
        exclude_ids = options['exclude_campaigns']
        exclude_all = options['exclude_all']
        dry_run = options['dry_run']

        # --- Read Excel ---
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

        # --- Deduplicate & validate ---
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

        # --- Build exclusion set ---
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

        final_emails = [e for e in valid_emails if e not in exclude_set]
        excluded_count = len(valid_emails) - len(final_emails)

        self.stdout.write(f'  After exclusion: {len(final_emails)} (excluded {excluded_count})')

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN — nothing created'))
            return

        if not final_emails:
            raise CommandError('No emails to send after exclusion')

        # --- Read HTML body ---
        body_html_file = options['body_html_file']
        try:
            with open(body_html_file, 'r', encoding='utf-8') as f:
                body_html = f.read()
        except Exception as e:
            raise CommandError(f'Cannot read HTML body file: {e}')

        body_text = ''
        if options['body_text_file']:
            try:
                with open(options['body_text_file'], 'r', encoding='utf-8') as f:
                    body_text = f.read()
            except Exception as e:
                raise CommandError(f'Cannot read text body file: {e}')

        # --- Create campaign ---
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

        # --- Create logs in bulk ---
        logs = [
            EmailLog(campaign=campaign, recipient_email=email)
            for email in final_emails
        ]
        EmailLog.objects.bulk_create(logs, batch_size=1000)

        self.stdout.write(self.style.SUCCESS(
            f'  ✅ Campaign #{campaign.pk} ready with {len(final_emails)} recipients\n'
            f'  To send: python manage.py send_campaign {campaign.pk}'
        ))
