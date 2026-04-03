import json
import urllib.parse

from django.core.paginator import Paginator
from django.db.models import Q
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from ..models import Seller, Review
from ..constants import SELLER_STATUS_ACTIVE
from .helpers import (
    _get_current_legacy_user, _is_admin_user, _no_store,
)
from .reviews import _get_reviews, _avg_points


def seller_list(request: HttpRequest) -> HttpResponse:
    q = (request.GET.get('q') or '').strip()
    qs = Seller.objects.select_related('user')
    if q:
        qs = qs.filter(Q(name__icontains=q) | Q(about__icontains=q) | Q(links__icontains=q))
    paginator = Paginator(qs.order_by('-created_at'), 12)
    page = paginator.get_page(request.GET.get('page') or 1)
    resp = render(
        request,
        'legacy/seller_list.html',
        {
            'sellers': page,
            'page_size': 12,
            'page_range': paginator.get_elided_page_range(page.number),
            'q': q,
            'legacy_user': _get_current_legacy_user(request),
        },
    )
    return _no_store(resp)


def seller_detail(request: HttpRequest, seller_id: int) -> HttpResponse:
    seller = get_object_or_404(Seller.objects.select_related('user'), pk=seller_id)
    try:
        contacts_display = json.dumps(seller.contacts, ensure_ascii=False, indent=2) if seller.contacts else ''
    except Exception:
        contacts_display = str(seller.contacts or '')

    legacy_user = _get_current_legacy_user(request)
    is_admin = _is_admin_user(legacy_user)
    reviews_qs = _get_reviews(Review.REVIEW_TYPE_SELLER, seller_id, include_moderation=is_admin)
    reviews_list = list(reviews_qs[:50])
    avg_rating = _avg_points(reviews_list)

    review_error = request.session.pop('review_error', '')
    review_success = request.session.pop('review_success', '')

    resp = render(
        request,
        'legacy/seller_detail.html',
        {
            'seller': seller,
            'contacts_display': contacts_display,
            'legacy_user': legacy_user,
            'reviews': reviews_list,
            'avg_rating': avg_rating,
            'review_error': review_error,
            'review_success': review_success,
            'review_type': Review.REVIEW_TYPE_SELLER,
            'review_object_id': seller_id,
            'is_admin_user': is_admin,
        },
    )
    return _no_store(resp)


def _parse_seller_form(post):
    name = (post.get('name') or '').strip()
    location = (post.get('location') or '').strip()
    contacts_raw = (post.get('contacts') or '').strip()
    links = (post.get('links') or '').strip()
    about = (post.get('about') or '').strip()

    errors = {}
    if not name:
        errors['name'] = 'Введите название'

    contacts = {}
    if contacts_raw:
        try:
            contacts = json.loads(contacts_raw)
            if not isinstance(contacts, dict):
                errors['contacts'] = 'Контакты должны быть JSON-объектом'
                contacts = {}
        except Exception:
            errors['contacts'] = 'Неверный формат JSON'

    initial = {'name': name, 'location': location, 'contacts': contacts_raw, 'links': links, 'about': about}
    cleaned = {'name': name, 'location': location, 'contacts': contacts, 'links': links, 'about': about}
    return cleaned, errors, initial


def seller_create(request: HttpRequest) -> HttpResponse:
    user = _get_current_legacy_user(request)
    if not user:
        return redirect(f"/login/?next={urllib.parse.quote(request.get_full_path())}")

    errors: dict = {}
    errors_all = ''
    initial: dict = {}

    if request.method == 'POST':
        cleaned, errors, initial = _parse_seller_form(request.POST)
        if not errors:
            if Seller.objects.filter(name=cleaned['name']).exists():
                errors['name'] = 'Предприятие с таким названием уже существует'
        if not errors:
            now = timezone.now()
            seller = Seller.objects.create(
                user_id=int(user.id),
                name=cleaned['name'],
                logo=0,
                location=cleaned['location'],
                contacts=cleaned['contacts'],
                price_list=0,
                links=cleaned['links'],
                about=cleaned['about'],
                created_at=now,
                updated_at=now,
                status=SELLER_STATUS_ACTIVE,
            )
            return redirect(f"/sellers/{int(seller.id)}/")

    resp = render(
        request,
        'legacy/seller_form.html',
        {'mode': 'create', 'seller': None, 'initial': initial, 'errors': errors, 'errors_all': errors_all, 'legacy_user': user},
    )
    return _no_store(resp)


def seller_edit(request: HttpRequest, seller_id: int) -> HttpResponse:
    user = _get_current_legacy_user(request)
    if not user:
        return redirect(f"/login/?next={urllib.parse.quote(request.get_full_path())}")
    seller = get_object_or_404(Seller.objects.select_related('user'), pk=seller_id)
    if not _is_admin_user(user) and int(seller.user_id) != int(user.id):
        return redirect(f"/sellers/{int(seller_id)}/")

    errors: dict = {}
    errors_all = ''

    if request.method == 'POST':
        cleaned, errors, initial = _parse_seller_form(request.POST)
        if not errors:
            dup = Seller.objects.filter(name=cleaned['name']).exclude(pk=int(seller_id)).exists()
            if dup:
                errors['name'] = 'Предприятие с таким названием уже существует'
        if not errors:
            Seller.objects.filter(pk=int(seller_id)).update(
                name=cleaned['name'],
                location=cleaned['location'],
                contacts=cleaned['contacts'],
                links=cleaned['links'],
                about=cleaned['about'],
                updated_at=timezone.now(),
            )
            return redirect(f"/sellers/{int(seller_id)}/")
    else:
        contacts_str = ''
        try:
            contacts_str = json.dumps(seller.contacts, ensure_ascii=False, indent=2) if seller.contacts else ''
        except Exception:
            contacts_str = ''
        initial = {
            'name': seller.name or '',
            'location': seller.location or '',
            'contacts': contacts_str,
            'links': seller.links or '',
            'about': seller.about or '',
        }

    resp = render(
        request,
        'legacy/seller_form.html',
        {'mode': 'edit', 'seller': seller, 'initial': initial, 'errors': errors, 'errors_all': errors_all, 'legacy_user': user},
    )
    return _no_store(resp)
