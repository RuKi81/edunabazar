"""URL-конфигурация ``my_fields``.

Заполняется на этапах 4-6 (API + UI). Сейчас — пустой include-роутер,
чтобы заранее смонтировать в ``enb_django/urls.py`` и не править его
повторно при добавлении первого view.
"""
from __future__ import annotations

from django.urls import path

app_name = 'my_fields'

urlpatterns: list = [
    # path('me/fields/', ...),                       # будет в этапе 5
    # path('me/fields/<int:pk>/', ...),              # этап 6
    # path('agrocosmos/api/my/fields/', ...),        # этап 4
]
