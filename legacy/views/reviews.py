import urllib.parse

from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect
from django.utils import timezone
from django.utils.http import url_has_allowed_host_and_scheme

from ..models import Review
from ..constants import (
    REVIEW_STATUS_DELETED, REVIEW_STATUS_HIDDEN, REVIEW_STATUS_MODERATION,
    REVIEW_STATUS_PUBLISHED,
)
from .helpers import (
    _get_current_legacy_user, _is_admin_user, _require_admin, logger,
)

_REVIEW_STATUS_PUBLISHED = REVIEW_STATUS_PUBLISHED
_REVIEW_STATUS_MODERATION = REVIEW_STATUS_MODERATION
_REVIEW_STATUS_HIDDEN = REVIEW_STATUS_HIDDEN
_REVIEW_STATUS_DELETED = REVIEW_STATUS_DELETED


def _get_reviews(review_type: int, object_id: int, include_moderation: bool = False):
    """Return published reviews for an object. If include_moderation, include status=5 too."""
    qs = Review.objects.select_related('author').filter(type=review_type, object_id=object_id)
    if include_moderation:
        qs = qs.filter(status__in=[_REVIEW_STATUS_PUBLISHED, _REVIEW_STATUS_MODERATION])
    else:
        qs = qs.filter(status=_REVIEW_STATUS_PUBLISHED)
    return qs.order_by('-created_at', '-id')


def _avg_points(reviews) -> float | None:
    """Calculate average points from a queryset/list of reviews."""
    total = 0
    count = 0
    for r in reviews:
        try:
            total += int(r.points)
            count += 1
        except Exception:
            pass
    return round(total / count, 1) if count else None


def review_create(request: HttpRequest) -> HttpResponse:
    """Create a review for an advert (type=0) or seller (type=1)."""
    if request.method != 'POST':
        return redirect('/adverts/')

    user = _get_current_legacy_user(request)
    if not user:
        return redirect(f"/login/?next={urllib.parse.quote(request.META.get('HTTP_REFERER', '/adverts/'))}")

    review_type_raw = (request.POST.get('review_type') or '').strip()
    object_id_raw = (request.POST.get('object_id') or '').strip()
    points_raw = (request.POST.get('points') or '').strip()
    text = (request.POST.get('text') or '').strip()

    try:
        review_type = int(review_type_raw)
        if review_type not in {Review.REVIEW_TYPE_ADVERT, Review.REVIEW_TYPE_SELLER}:
            review_type = Review.REVIEW_TYPE_ADVERT
    except Exception:
        review_type = Review.REVIEW_TYPE_ADVERT

    try:
        object_id = int(object_id_raw)
    except Exception:
        return redirect('/adverts/')

    try:
        points = int(points_raw)
        points = max(1, min(5, points))
    except Exception:
        points = 5

    if not text:
        request.session['review_error'] = 'Введите текст отзыва'
        if review_type == Review.REVIEW_TYPE_SELLER:
            return redirect(f"/sellers/{object_id}/")
        return redirect(f"/adverts/{object_id}/")

    if len(text) > 2000:
        text = text[:2000]

    # Prevent duplicate: one review per user per object
    existing = Review.objects.filter(
        type=review_type, object_id=object_id, author_id=user.id,
    ).exclude(status=_REVIEW_STATUS_DELETED).exists()
    if existing:
        request.session['review_error'] = 'Вы уже оставили отзыв'
        if review_type == Review.REVIEW_TYPE_SELLER:
            return redirect(f"/sellers/{object_id}/")
        return redirect(f"/adverts/{object_id}/")

    now = timezone.now()
    Review.objects.create(
        type=review_type,
        object_id=object_id,
        points=points,
        author_id=int(user.id),
        text=text,
        created_at=now,
        updated_at=now,
        status=_REVIEW_STATUS_MODERATION,
    )

    request.session['review_success'] = 'Отзыв отправлен на модерацию'
    if review_type == Review.REVIEW_TYPE_SELLER:
        return redirect(f"/sellers/{object_id}/")
    return redirect(f"/adverts/{object_id}/")


def review_delete(request: HttpRequest, review_id: int) -> HttpResponse:
    """Delete (soft) a review. Author or admin only."""
    if request.method != 'POST':
        return redirect('/adverts/')

    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/')

    review = get_object_or_404(Review, pk=review_id)
    is_author = int(review.author_id) == int(user.id)
    is_admin = _is_admin_user(user)
    if not is_author and not is_admin:
        return redirect('/adverts/')

    Review.objects.filter(pk=review_id).update(status=_REVIEW_STATUS_DELETED, updated_at=timezone.now())

    next_url = (request.POST.get('next') or '').strip()
    if next_url and url_has_allowed_host_and_scheme(
        url=next_url, allowed_hosts={request.get_host()}, require_https=request.is_secure(),
    ):
        return redirect(next_url)
    return redirect('/adverts/')


def review_publish(request: HttpRequest, review_id: int) -> HttpResponse:
    """Publish a review (admin only)."""
    if request.method != 'POST':
        return redirect('/adverts/')

    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    Review.objects.filter(pk=review_id).update(status=_REVIEW_STATUS_PUBLISHED, updated_at=timezone.now())

    next_url = (request.POST.get('next') or '').strip()
    if next_url and url_has_allowed_host_and_scheme(
        url=next_url, allowed_hosts={request.get_host()}, require_https=request.is_secure(),
    ):
        return redirect(next_url)
    return redirect('/adverts/')


def review_hide(request: HttpRequest, review_id: int) -> HttpResponse:
    """Hide a review (admin only)."""
    if request.method != 'POST':
        return redirect('/adverts/')

    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    Review.objects.filter(pk=review_id).update(status=_REVIEW_STATUS_HIDDEN, updated_at=timezone.now())

    next_url = (request.POST.get('next') or '').strip()
    if next_url and url_has_allowed_host_and_scheme(
        url=next_url, allowed_hosts={request.get_host()}, require_https=request.is_secure(),
    ):
        return redirect(next_url)
    return redirect('/adverts/')
