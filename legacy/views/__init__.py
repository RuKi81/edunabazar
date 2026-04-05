"""
legacy.views package — split from the monolithic views.py.

Re-exports every public name so that existing imports
(e.g. ``from .views import ...`` or ``from . import views``)
continue to work without changes.
"""

# ── helpers (used by api.py, tests.py, and other modules) ──────────────────
from .helpers import (                                          # noqa: F401
    _get_current_legacy_user,
    _is_admin_user,
    _normalize_phone,
    _normalize_extra_contacts,
    _EXTRA_CONTACT_LABELS,
)

# ── middleware decorator ───────────────────────────────────────────────────
from ..middleware import legacy_login_required                   # noqa: F401

# ── pages ──────────────────────────────────────────────────────────────────
from .pages import (                                            # noqa: F401
    about, contacts, howto, prices_page, news_detail, home,
)

# ── adverts ────────────────────────────────────────────────────────────────
from .adverts import (                                          # noqa: F401
    advert_list, advert_detail, advert_create, advert_edit,
    advert_hide, advert_publish, advert_bump, advert_delete,
    _parse_advert_form,
)

# ── sellers ────────────────────────────────────────────────────────────────
from .sellers import (                                          # noqa: F401
    seller_list, seller_detail, seller_create, seller_edit,
)

# ── map & geocoding ───────────────────────────────────────────────────────
from .map import (                                              # noqa: F401
    map_view, map_adverts_api, map_categories_api,
    geocode_api, reverse_geocode_api,
)

# ── auth ──────────────────────────────────────────────────────────────────
from .auth import (                                             # noqa: F401
    legacy_login, legacy_logout,
    legacy_register_start, legacy_register_email,
    legacy_register_sms, legacy_register_sms_confirm,
    legacy_set_password, change_password,
)

# ── profile ───────────────────────────────────────────────────────────────
from .profile import legacy_me, legacy_me_bulk_adverts          # noqa: F401

# ── messages ──────────────────────────────────────────────────────────────
from .messages import (                                         # noqa: F401
    messages_inbox, messages_thread, message_send,
    messages_unread_count_api,
)

# ── reviews ───────────────────────────────────────────────────────────────
from .reviews import (                                          # noqa: F401
    review_create, review_delete, review_publish, review_hide,
)

# ── favorites ─────────────────────────────────────────────────────────────
from .favorites import favorite_toggle, favorites_list          # noqa: F401

# ── regions ──────────────────────────────────────────────────────────────
from .regions import region_detail, region_list                  # noqa: F401

# ── admin ─────────────────────────────────────────────────────────────────
from .admin_views import (                                      # noqa: F401
    admin_users, admin_users_bulk_delete, admin_catalogs, admin_user_detail,
    admin_reviews, admin_review_action,
    admin_messages, admin_message_delete,
    admin_campaigns, admin_campaign_create, admin_campaign_detail,
    admin_campaign_delete, admin_campaign_send_test,
    admin_campaign_upload_excel, admin_campaign_send_batch,
)
