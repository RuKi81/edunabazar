"""HTML-view для ``my_fields``.

REST API (``GET/POST/PATCH/DELETE``) живёт в ``my_fields.api``. Здесь —
только страницы, которые отдают HTML с встроенным Leaflet'ом, а вся
последующая интерактивность бьёт в API через ``fetch``.

Решение «server-rendered shell + client-side AJAX» выбрано вместо
полноценного SPA по двум причинам:

* остальной кабинет в проекте — серверный (см. ``legacy/me.html``,
  ``agrocosmos/cabinet.html``), и единый стек удобнее в поддержке;
* для MVP-1 (геометрия + журнал) нет нужды в роутере / state-менеджере.

Когда в фазе 2-3 появятся графики NDVI и прогноз погоды, при желании
можно подмешать Alpine.js или HTMX — но не сейчас.
"""
from __future__ import annotations

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, render

from .models import UserField
from .permissions import can_view_field
from .services.quotas import get_user_plan


@login_required(login_url='/login/')
def fields_list_page(request: HttpRequest) -> HttpResponse:
    """Страница «Мои поля» — карта + список + кнопка рисования.

    Сами поля подгружаются с ``/api/my/fields/`` JS-ом, чтобы не
    дублировать GeoJSON-сериализацию в шаблоне. Контекст содержит лишь
    данные для шапки (текущий тариф + квоты).
    """
    plan = get_user_plan(request.user)
    fields_count = UserField.objects.filter(
        owner=request.user, is_archived=False,
    ).count()
    return render(request, 'my_fields/fields_list.html', {
        'active_section': 'my_fields',
        'plan': plan,
        'fields_count': fields_count,
    })


@login_required(login_url='/login/')
def field_detail_page(request: HttpRequest, pk: int) -> HttpResponse:
    """Карточка одного поля — карта + журнал + сезоны."""
    field = get_object_or_404(
        UserField.objects.select_related('region', 'district'), pk=pk,
    )
    if not can_view_field(request.user, field):
        # 404 а не 403 — чтобы не «утекало» наличие чужого поля.
        from django.http import Http404
        raise Http404
    return render(request, 'my_fields/field_detail.html', {
        'active_section': 'my_fields',
        'field': field,
    })
