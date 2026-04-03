import urllib.parse

from django.db.models import Prefetch
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render

from ..models import Advert, AdvertPhoto, Favorite
from ..constants import ADVERT_STATUS_PUBLISHED
from .helpers import _get_current_legacy_user, _no_store


def favorite_toggle(request: HttpRequest, advert_id: int) -> JsonResponse:
    """Toggle favorite status for an advert. Returns JSON."""
    user = _get_current_legacy_user(request)
    if not user:
        return JsonResponse({'ok': False, 'error': 'auth'}, status=401)

    advert = Advert.objects.filter(pk=advert_id).first()
    if not advert:
        return JsonResponse({'ok': False, 'error': 'not_found'}, status=404)

    fav, created = Favorite.objects.get_or_create(user=user, advert_id=advert_id)
    if not created:
        fav.delete()

    return JsonResponse({'ok': True, 'is_favorited': created})


def favorites_list(request: HttpRequest) -> HttpResponse:
    """Show user's favorited adverts."""
    user = _get_current_legacy_user(request)
    if not user:
        return redirect(f"/login/?next={urllib.parse.quote(request.get_full_path())}")

    _thumb_prefetch = Prefetch(
        'photos',
        queryset=AdvertPhoto.objects.order_by('sort', 'id'),
        to_attr='prefetched_photos',
    )
    fav_ids = Favorite.objects.filter(user=user).order_by('-created_at').values_list('advert_id', flat=True)
    adverts = list(
        Advert.objects.filter(pk__in=fav_ids, status=ADVERT_STATUS_PUBLISHED)
        .select_related('category', 'author')
        .prefetch_related(_thumb_prefetch)
    )
    # Preserve favorites order
    order_map = {aid: i for i, aid in enumerate(fav_ids)}
    adverts.sort(key=lambda a: order_map.get(a.id, 0))

    return _no_store(render(request, 'legacy/favorites.html', {
        'adverts': adverts,
        'legacy_user': user,
    }))
