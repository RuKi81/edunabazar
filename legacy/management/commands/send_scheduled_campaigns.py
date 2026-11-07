"""
Ежедневная автоотправка групп email-кампаний.

Запускается из cron каждые несколько минут. Для каждой кампании с включённым
флагом «Отправлять каждый день» проверяет: сегодня (по Москве) ещё не было
автоотправки и текущее время >= времени отправки. Если так — отправляет
ОДНУ очередную группу (первую по порядку с неотправленными письмами) тем же
кодом, что и кнопка «Отправить» в админке. Когда групп для отправки не
осталось, флаг снимается.

Защита от повторного запуска в тот же день — атомарное проставление
``auto_send_last_date`` ДО начала отправки.
"""
import logging

from django.core.management.base import BaseCommand
from django.utils import timezone

from legacy.campaign_batches import next_pending_batch
from legacy.models import EmailCampaign
from legacy.views.admin_views import send_batch_range

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Отправить очередную группу писем кампаний с включённой ежедневной автоотправкой'

    def handle(self, *args, **options):
        now = timezone.localtime()
        today = now.date()
        campaigns = EmailCampaign.objects.filter(auto_send_enabled=True).order_by('id')

        for campaign in campaigns:
            if campaign.auto_send_last_date == today:
                continue
            if now.time() < campaign.auto_send_time:
                continue
            self._process(campaign, today)

    def _disable(self, campaign, reason: str) -> None:
        campaign.auto_send_enabled = False
        campaign.save(update_fields=['auto_send_enabled'])
        self.stdout.write(f'Campaign #{campaign.pk}: auto-send disabled — {reason}')

    def _process(self, campaign, today) -> None:
        batch = next_pending_batch(campaign)
        if batch is None:
            self._disable(campaign, 'no pending batches')
            return

        claimed = (
            EmailCampaign.objects
            .filter(pk=campaign.pk, auto_send_enabled=True)
            .exclude(auto_send_last_date=today)
            .update(auto_send_last_date=today)
        )
        if not claimed:
            return

        self.stdout.write(
            f'Campaign #{campaign.pk}: sending batch {batch["num"]} '
            f'({batch["pending"]} pending)...'
        )
        try:
            sent, failed = send_batch_range(campaign, batch['first_id'], batch['last_id'])
        except OSError as e:
            logger.error('Campaign #%s: SMTP connect error: %s', campaign.pk, e)
            self.stderr.write(f'Campaign #{campaign.pk}: SMTP connect error: {e}')
            return

        self.stdout.write(self.style.SUCCESS(
            f'Campaign #{campaign.pk}: batch {batch["num"]} sent={sent} failed={failed}'
        ))

        if next_pending_batch(campaign) is None:
            self._disable(campaign, 'all batches sent')
