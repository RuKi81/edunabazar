"""Реестр стран Агрокосмоса.

``Region`` — субъект (первый административный уровень) одной из стран
реестра; страна хранится в ``Region.country`` (ISO 3166-1 alpha-2).
Везде, где страна не указана явно, подразумевается ``DEFAULT_COUNTRY``
(Россия): так селекты, отчёты и кэши, написанные до появления других
стран, продолжают показывать только субъекты РФ.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Country:
    code: str            # ISO 3166-1 alpha-2, префикс OSM-тега ISO3166-2
    name: str            # «Казахстан» — подпись в селекте
    name_full: str       # именительный падеж для заголовков отчёта
    name_gen: str        # родительный падеж: «угодий Казахстана»
    # OSM admin_level районов (второй уровень). None — районы не грузим:
    # по стране доступны только границы субъектов и слой угодий.
    district_admin_level: int | None
    # Страна стоит на оперативном MODIS-мониторинге (cron check_monitoring).
    monitored: bool
    # Экстент карты по умолчанию: ((юг, запад), (север, восток)).
    bounds: tuple[tuple[float, float], tuple[float, float]]


DEFAULT_COUNTRY = 'RU'

COUNTRIES: tuple[Country, ...] = (
    Country('RU', 'Россия', 'Российская Федерация', 'Российской Федерации',
            6, True, ((43.0, 19.0), (68.0, 65.0))),
    Country('KZ', 'Казахстан', 'Казахстан', 'Казахстана',
            6, True, ((40.5, 46.5), (55.5, 87.5))),
    Country('BY', 'Белоруссия', 'Белоруссия', 'Белоруссии',
            6, True, ((51.2, 23.1), (56.2, 32.8))),
    Country('IN', 'Индия', 'Индия', 'Индии',
            None, False, ((6.5, 68.0), (36.0, 97.5))),
    Country('CN', 'Китай', 'Китай', 'Китая',
            None, False, ((18.0, 73.5), (53.6, 135.1))),
    Country('BR', 'Бразилия', 'Бразилия', 'Бразилии',
            None, False, ((-33.8, -74.0), (5.3, -34.8))),
)

_BY_CODE = {c.code: c for c in COUNTRIES}

COUNTRY_CHOICES = [(c.code, c.name) for c in COUNTRIES]
MONITORED_COUNTRIES = tuple(c.code for c in COUNTRIES if c.monitored)


def normalize_country(raw) -> str | None:
    """``'kz'`` → ``'KZ'``; пусто или неизвестный код → ``None``."""
    code = (raw or '').strip().upper()
    return code if code in _BY_CODE else None


def get_country(raw=None) -> Country:
    """Страна по коду; пусто или неизвестный код → Россия."""
    return _BY_CODE[normalize_country(raw) or DEFAULT_COUNTRY]
