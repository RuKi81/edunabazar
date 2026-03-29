"""
Management command to send an email campaign.

Usage:
    python manage.py send_campaign <campaign_id> [--rate=1] [--batch=50] [--resume]

Features:
- Throttled sending (default 1 email/sec for Yandex Cloud Postbox quota)
- Resume support (skips already-sent emails)
- Retry on transient SMTP errors
- Real-time progress output
- Graceful pause on Ctrl+C (sets campaign to 'paused')
"""

import logging
import signal
import time

from django.conf import settings
from django.core.mail import EmailMultiAlternatives, get_connection
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from legacy.models import EmailCampaign, EmailLog, LegacyUser

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Send an email campaign to recipients'

    def add_arguments(self, parser):
        parser.add_argument('campaign_id', type=int, help='ID of the EmailCampaign to send')
        parser.add_argument('--rate', type=float, default=1.0,
                            help='Max emails per second (default: 1)')
        parser.add_argument('--batch', type=int, default=50,
                            help='SMTP connection batch size before reconnecting (default: 50)')
        parser.add_argument('--resume', action='store_true',
                            help='Resume a paused campaign')
        parser.add_argument('--dry-run', action='store_true',
                            help='Populate logs but do not actually send emails')
        parser.add_argument('--limit', type=int, default=0,
                            help='Max emails to send in this run (0 = unlimited)')

    def handle(self, *args, **options):
        campaign_id = options['campaign_id']
        rate = options['rate']
        batch_size = options['batch']
        resume = options['resume']
        dry_run = options['dry_run']
        limit = options['limit']

        try:
            campaign = EmailCampaign.objects.get(pk=campaign_id)
        except EmailCampaign.DoesNotExist:
            raise CommandError(f'Campaign #{campaign_id} not found')

        if campaign.status == EmailCampaign.STATUS_DONE:
            raise CommandError(f'Campaign #{campaign_id} is already done')

        if campaign.status == EmailCampaign.STATUS_SENDING and not resume:
            raise CommandError(
                f'Campaign #{campaign_id} is already sending. '
                f'Use --resume to continue.'
            )

        if campaign.status == EmailCampaign.STATUS_DRAFT:
            self._populate_logs(campaign)

        if campaign.status == EmailCampaign.STATUS_PAUSED and not resume:
            raise CommandError(
                f'Campaign #{campaign_id} is paused. Use --resume to continue.'
            )

        # Set up graceful stop on Ctrl+C
        self._stop_requested = False

        def _signal_handler(sig, frame):
            self.stderr.write('\n⏸  Ctrl+C received — pausing campaign...')
            self._stop_requested = True

        old_handler = signal.signal(signal.SIGINT, _signal_handler)

        try:
            self._send_campaign(campaign, rate, batch_size, dry_run, limit)
        finally:
            signal.signal(signal.SIGINT, old_handler)

    def _populate_logs(self, campaign):
        """Create EmailLog entries for all recipients based on audience."""
        self.stdout.write(f'Populating recipient list for campaign #{campaign.pk}...')

        emails = self._get_recipient_emails(campaign)

        # Deduplicate
        seen = set()
        unique_emails = []
        for e in emails:
            lower = e.strip().lower()
            if lower and lower not in seen:
                seen.add(lower)
                unique_emails.append(lower)

        # Create logs in bulk
        logs = [
            EmailLog(campaign=campaign, recipient_email=email)
            for email in unique_emails
        ]
        EmailLog.objects.bulk_create(logs, batch_size=1000)

        campaign.total_recipients = len(unique_emails)
        campaign.status = EmailCampaign.STATUS_SENDING
        campaign.started_at = timezone.now()
        campaign.save(update_fields=['total_recipients', 'status', 'started_at'])

        self.stdout.write(self.style.SUCCESS(
            f'  Created {len(unique_emails)} recipient logs'
        ))

    def _get_recipient_emails(self, campaign):
        """Return list of email addresses based on campaign audience."""
        qs = LegacyUser.objects.exclude(email='').exclude(email__isnull=True)

        if campaign.audience == EmailCampaign.AUDIENCE_REGISTERED:
            qs = qs.filter(status=10)
        elif campaign.audience == EmailCampaign.AUDIENCE_IMPORTED:
            qs = qs.exclude(status=10)

        return list(qs.values_list('email', flat=True))

    def _send_campaign(self, campaign, rate, batch_size, dry_run, limit=0):
        """Send pending emails with throttling."""
        campaign.status = EmailCampaign.STATUS_SENDING
        campaign.save(update_fields=['status'])

        from_email = campaign.from_email or settings.DEFAULT_FROM_EMAIL
        delay = 1.0 / rate if rate > 0 else 1.0

        pending_logs = EmailLog.objects.filter(
            campaign=campaign,
            status=EmailLog.STATUS_PENDING,
        ).order_by('id')

        total_pending = pending_logs.count()
        run_limit = min(limit, total_pending) if limit > 0 else total_pending
        self.stdout.write(
            f'Sending campaign #{campaign.pk}: '
            f'{total_pending} pending of {campaign.total_recipients} total'
        )
        if limit > 0:
            self.stdout.write(f'  Limit this run: {run_limit} emails')
        if dry_run:
            self.stdout.write(self.style.WARNING('  DRY RUN — no emails will be sent'))

        sent = 0
        failed = 0
        connection = None
        batch_counter = 0

        for log in pending_logs.iterator():
            if self._stop_requested:
                break

            if limit > 0 and (sent + failed) >= limit:
                self.stdout.write(f'  Reached limit of {limit} emails for this run')
                break

            # Open/reopen SMTP connection every batch_size emails
            if connection is None or batch_counter >= batch_size:
                if connection:
                    try:
                        connection.close()
                    except Exception:
                        pass
                if not dry_run:
                    connection = get_connection()
                    connection.open()
                batch_counter = 0

            try:
                if not dry_run:
                    msg = EmailMultiAlternatives(
                        subject=campaign.subject,
                        body=campaign.body_text or campaign.subject,
                        from_email=from_email,
                        to=[log.recipient_email],
                        connection=connection,
                    )
                    if campaign.body_html:
                        msg.attach_alternative(campaign.body_html, 'text/html')
                    msg.send(fail_silently=False)

                log.status = EmailLog.STATUS_SENT
                log.sent_at = timezone.now()
                log.save(update_fields=['status', 'sent_at'])
                sent += 1
                batch_counter += 1

            except Exception as e:
                error_msg = str(e)[:500]
                log.status = EmailLog.STATUS_FAILED
                log.error_message = error_msg
                log.save(update_fields=['status', 'error_message'])
                failed += 1
                logger.warning('Failed to send to %s: %s', log.recipient_email, error_msg)

            # Progress
            total_done = sent + failed
            if total_done % 100 == 0 or total_done == total_pending:
                self.stdout.write(
                    f'  [{total_done}/{total_pending}] '
                    f'sent={sent} failed={failed}'
                )

            # Throttle
            time.sleep(delay)

        # Close connection
        if connection:
            try:
                connection.close()
            except Exception:
                pass

        # Update campaign counters
        campaign.sent_count = EmailLog.objects.filter(
            campaign=campaign, status=EmailLog.STATUS_SENT
        ).count()
        campaign.failed_count = EmailLog.objects.filter(
            campaign=campaign, status=EmailLog.STATUS_FAILED
        ).count()

        if self._stop_requested:
            campaign.status = EmailCampaign.STATUS_PAUSED
            campaign.save(update_fields=['sent_count', 'failed_count', 'status'])
            self.stdout.write(self.style.WARNING(
                f'\n⏸  Campaign paused. sent={campaign.sent_count} failed={campaign.failed_count}'
                f'\n   Resume with: python manage.py send_campaign {campaign.pk} --resume'
            ))
        else:
            remaining = EmailLog.objects.filter(
                campaign=campaign, status=EmailLog.STATUS_PENDING
            ).count()
            if remaining == 0:
                campaign.status = EmailCampaign.STATUS_DONE
                campaign.finished_at = timezone.now()
            campaign.save(update_fields=['sent_count', 'failed_count', 'status', 'finished_at'])
            self.stdout.write(self.style.SUCCESS(
                f'\n✅ Campaign #{campaign.pk} done. '
                f'sent={campaign.sent_count} failed={campaign.failed_count}'
            ))
