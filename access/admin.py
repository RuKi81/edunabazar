"""Админка гранта доступа + матрица прав к ГИС-слоям.

Матрица (`/admin/access/resourcegrant/matrix/`) — таблица «пользователи ×
ресурсы»: в шапке колонки «Все ГИС-слои» (whole-class) и каждый загруженный
слой, под каждым — чекбоксы уровней view/edit/manage. Уровни иерархичны
(view<edit<manage), поэтому чекбоксы кумулятивны и при сохранении хранится
единственный ``ResourceGrant`` на пару (пользователь, ресурс) с максимальным
отмеченным уровнем (unique_together этого требует).
"""
from django.contrib import admin
from django.db.models import Q
from django.shortcuts import redirect, render
from django.urls import path, reverse

from legacy.models import LegacyUser

from .models import ResourceGrant

_LEVELS = ('view', 'edit', 'manage')
_LEVEL_ORDER = {'view': 1, 'edit': 2, 'manage': 3}


@admin.register(ResourceGrant)
class ResourceGrantAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'legacy_user', 'resource_type', 'resource_id', 'level',
        'granted_by', 'created_at',
    )
    list_filter = ('resource_type', 'level')
    search_fields = (
        'legacy_user__username', 'legacy_user__email', 'note',
        'resource_id',
    )
    raw_id_fields = ('legacy_user', 'granted_by')
    readonly_fields = ('created_at',)
    ordering = ('-created_at',)
    change_list_template = 'admin/access/resourcegrant/change_list.html'

    # ── матрица прав ────────────────────────────────────────────────────
    def get_urls(self):
        urls = super().get_urls()
        custom = [
            path(
                'matrix/', self.admin_site.admin_view(self.matrix_view),
                name='access_resourcegrant_matrix',
            ),
        ]
        return custom + urls

    def _gis_layers(self):
        from my_fields.models import GisLayer
        return list(GisLayer.objects.order_by('sort_order', 'id'))

    def _search_users(self, query):
        """Найденные по строке поиска пользователи (username/email/имя/id)."""
        q = (query or '').strip()
        if not q:
            return []
        cond = Q(username__icontains=q) | Q(email__icontains=q) | Q(name__icontains=q)
        if q.isdigit():
            cond = cond | Q(pk=int(q))
        return list(
            LegacyUser.objects.filter(cond).order_by('username')[:50]
        )

    def _matrix_users(self, extra_query=''):
        """Кандидаты для выбора: пользователи с ГИС-грантами + найденные."""
        uids = set(
            ResourceGrant.objects.filter(
                resource_type=ResourceGrant.ResourceType.GIS_LAYER,
            ).values_list('legacy_user_id', flat=True)
        )
        found_ids = {u.id for u in self._search_users(extra_query)}
        all_ids = uids | found_ids
        return list(LegacyUser.objects.filter(id__in=all_ids).order_by('username'))

    def _current_levels(self):
        """{(user_id, scope): level} для gis_layer-грантов, scope='all'|<layer_id>."""
        out = {}
        for uid, rid, level in ResourceGrant.objects.filter(
            resource_type=ResourceGrant.ResourceType.GIS_LAYER,
        ).values_list('legacy_user_id', 'resource_id', 'level'):
            scope = 'all' if rid is None else str(rid)
            out[(uid, scope)] = level
        return out

    def matrix_view(self, request):
        if not self.has_change_permission(request):
            from django.core.exceptions import PermissionDenied
            raise PermissionDenied

        layers = self._gis_layers()
        scopes = ['all'] + [str(x.pk) for x in layers]

        if request.method == 'POST':
            user_ids = [
                int(x) for x in request.POST.getlist('user_ids') if x.isdigit()
            ]
            self._save_matrix(request, user_ids, scopes)
            self.message_user(request, 'Матрица доступа сохранена.')
            url = reverse('admin:access_resourcegrant_matrix')
            return redirect(url)

        query = request.GET.get('u', '')
        pick_users = self._matrix_users(query)
        found = self._search_users(query) if query else []

        # Выбранный пользователь: явный ?uid=, иначе первый найденный поиском.
        selected = None
        uid_param = request.GET.get('uid', '')
        if uid_param.isdigit():
            selected = LegacyUser.objects.filter(pk=int(uid_param)).first()
        if selected is None and found:
            selected = found[0]

        current = self._current_levels()

        # Строки таблицы: «Все ГИС-слои» + каждый слой (вертикально).
        scope_defs = [{'scope': 'all', 'title': 'Все ГИС-слои'}]
        scope_defs += [
            {'scope': str(layer.pk), 'title': layer.title} for layer in layers
        ]

        rows = []
        for sd in scope_defs:
            boxes = None
            if selected is not None:
                lvl = current.get((selected.id, sd['scope']))
                order = _LEVEL_ORDER.get(lvl, 0)
                boxes = [{
                    'level': lv,
                    'name': f'g_{selected.id}_{sd["scope"]}_{lv}',
                    'checked': _LEVEL_ORDER[lv] <= order,
                } for lv in _LEVELS]
            rows.append({'scope': sd['scope'], 'title': sd['title'], 'boxes': boxes})

        context = {
            **self.admin_site.each_context(request),
            'title': 'Матрица доступа к ГИС-слоям',
            'opts': self.model._meta,
            'layers': layers,
            'level_heads': [
                {'level': 'view', 'label': 'Просмотр'},
                {'level': 'edit', 'label': 'Редакт.'},
                {'level': 'manage', 'label': 'Управл.'},
            ],
            'rows': rows,
            'pick_users': pick_users,
            'selected': selected,
            'query': query,
        }
        return render(
            request, 'admin/access/resourcegrant/matrix.html', context)

    def _save_matrix(self, request, user_ids, scopes):
        """Сверить чекбоксы с БД: upsert максимального уровня или удаление."""
        granter = getattr(request, 'legacy_user', None)
        granter_id = getattr(granter, 'id', None)
        gis = ResourceGrant.ResourceType.GIS_LAYER
        for uid in user_ids:
            for scope in scopes:
                # Максимальный отмеченный уровень для (user, scope).
                top = 0
                for lv in _LEVELS:
                    if request.POST.get(f'g_{uid}_{scope}_{lv}'):
                        top = max(top, _LEVEL_ORDER[lv])
                resource_id = None if scope == 'all' else int(scope)
                existing = ResourceGrant.objects.filter(
                    legacy_user_id=uid, resource_type=gis,
                    resource_id=resource_id,
                ).first()
                if top == 0:
                    if existing:
                        existing.delete()
                    continue
                level = next(lv for lv in _LEVELS if _LEVEL_ORDER[lv] == top)
                if existing:
                    if existing.level != level:
                        existing.level = level
                        existing.save(update_fields=['level'])
                else:
                    ResourceGrant.objects.create(
                        legacy_user_id=uid, resource_type=gis,
                        resource_id=resource_id, level=level,
                        granted_by_id=granter_id,
                    )
