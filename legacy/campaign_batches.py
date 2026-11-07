"""Разбиение получателей email-кампании на группы фиксированного размера.

Используется страницей кампании в админке (таблица «Группы рассылки») и
командой ``send_scheduled_campaigns`` (ежедневная автоотправка очередной
группы). Группы строятся по возрастанию ``EmailLog.id``, поэтому порядок
стабилен между вызовами и совпадает с порядком загрузки списка.
"""
from .models import EmailLog

BATCH_SIZE = 180


def build_batches(campaign, batch_size: int | None = None) -> list[dict]:
    batch_size = batch_size or BATCH_SIZE
    all_logs = list(
        EmailLog.objects.filter(campaign=campaign)
        .order_by('id')
        .values_list('id', 'status', 'sent_at', named=True)
    )
    batches = []
    for i in range(0, len(all_logs), batch_size):
        chunk = all_logs[i:i + batch_size]
        b_sent = sum(1 for r in chunk if r.status == EmailLog.STATUS_SENT)
        b_failed = sum(1 for r in chunk if r.status == EmailLog.STATUS_FAILED)
        b_pending = sum(1 for r in chunk if r.status == EmailLog.STATUS_PENDING)
        sent_ats = [r.sent_at for r in chunk if r.sent_at]
        batches.append({
            'num': (i // batch_size) + 1,
            'total': len(chunk),
            'sent': b_sent,
            'failed': b_failed,
            'pending': b_pending,
            'sent_at': max(sent_ats) if sent_ats else None,
            'first_id': chunk[0].id,
            'last_id': chunk[-1].id,
            'done': b_pending == 0,
        })
    return batches


def next_pending_batch(campaign, batch_size: int | None = None) -> dict | None:
    """Первая по порядку группа, в которой ещё есть неотправленные письма."""
    for batch in build_batches(campaign, batch_size):
        if not batch['done']:
            return batch
    return None
