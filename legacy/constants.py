"""
Shared constants for the legacy app.
Advert / Review / Seller / User status codes and helper mappings.
"""

# ---------------------------------------------------------------------------
# Advert statuses
# ---------------------------------------------------------------------------
ADVERT_STATUS_DELETED = 0
ADVERT_STATUS_HIDDEN = 3
ADVERT_STATUS_MODERATION = 5
ADVERT_STATUS_PUBLISHED = 10

ADVERT_STATUS_LABELS = {
    ADVERT_STATUS_PUBLISHED: 'Опубликовано',
    ADVERT_STATUS_MODERATION: 'На модерации',
    ADVERT_STATUS_HIDDEN: 'Скрыто',
    ADVERT_STATUS_DELETED: 'Удалено',
}

# Statuses visible to the advert author (not deleted)
ADVERT_VISIBLE_STATUSES = {ADVERT_STATUS_PUBLISHED, ADVERT_STATUS_MODERATION, ADVERT_STATUS_HIDDEN}

# ---------------------------------------------------------------------------
# Review statuses
# ---------------------------------------------------------------------------
REVIEW_STATUS_DELETED = 0
REVIEW_STATUS_HIDDEN = 3
REVIEW_STATUS_MODERATION = 5
REVIEW_STATUS_PUBLISHED = 10

# ---------------------------------------------------------------------------
# User statuses
# ---------------------------------------------------------------------------
USER_STATUS_ACTIVE = 10

# ---------------------------------------------------------------------------
# Seller statuses
# ---------------------------------------------------------------------------
SELLER_STATUS_ACTIVE = 10
