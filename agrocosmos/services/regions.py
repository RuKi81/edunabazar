"""Поиск региона по коду субъекта РФ для management-команд."""
from django.core.management.base import CommandError

from agrocosmos.models import Region


def add_subject_code_argument(parser):
    parser.add_argument(
        '--subject-code', type=str, default=None,
        help='Код субъекта РФ (71 — Тульская область, 50 — Московская) '
             'вместо --region-id. Не путать: --region-id — внутренний '
             'номер записи, с кодом субъекта он не совпадает.',
    )


def apply_subject_code(options):
    """Перевести ``--subject-code`` в ``options['region_id']``.

    Код субъекта и ``--region-id`` вместе — ошибка: это разные номера, и
    молча выбрать один из них значило бы посчитать не тот регион.
    """
    raw = options.get('subject_code')
    if not raw:
        return
    if options.get('region_id'):
        raise CommandError(
            'Укажите что-то одно: --subject-code или --region-id.')
    subject_code = str(raw).strip().zfill(2)
    region = Region.objects.filter(subject_code=subject_code).first()
    if region is None:
        raise CommandError(
            f'Регион с кодом субъекта {subject_code} не найден.')
    options['region_id'] = region.pk
