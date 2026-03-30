"""
Image processing utilities: compress originals, generate WebP thumbnails.
"""

import io
import os
import uuid

from django.core.files.base import ContentFile
from PIL import Image as PILImage, ImageOps

# Max dimensions
ORIGINAL_MAX_WIDTH = 1200
ORIGINAL_MAX_HEIGHT = 1200
ORIGINAL_QUALITY = 85

THUMB_WIDTH = 400
THUMB_HEIGHT = 300
THUMB_QUALITY = 80


def process_uploaded_image(uploaded_file):
    """
    Process an uploaded image file:
    1. Compress/resize the original (JPEG, max 1200px)
    2. Generate a WebP thumbnail (400x300)

    Returns: (compressed_original: ContentFile, thumbnail: ContentFile)
    """
    try:
        img = PILImage.open(uploaded_file)
    except Exception:
        # Not a valid image — return as-is with no thumbnail
        uploaded_file.seek(0)
        return uploaded_file, None

    # Fix orientation from EXIF
    img = ImageOps.exif_transpose(img)

    # Convert to RGB if needed (for JPEG/WebP output)
    if img.mode in ('RGBA', 'P', 'LA'):
        background = PILImage.new('RGB', img.size, (255, 255, 255))
        if img.mode == 'P':
            img = img.convert('RGBA')
        background.paste(img, mask=img.split()[-1] if 'A' in img.mode else None)
        img = background
    elif img.mode != 'RGB':
        img = img.convert('RGB')

    # --- Compress original ---
    original = img.copy()
    original.thumbnail((ORIGINAL_MAX_WIDTH, ORIGINAL_MAX_HEIGHT), PILImage.LANCZOS)

    buf_orig = io.BytesIO()
    original.save(buf_orig, format='JPEG', quality=ORIGINAL_QUALITY, optimize=True)
    buf_orig.seek(0)

    orig_name = f'{uuid.uuid4().hex[:12]}.jpg'
    compressed = ContentFile(buf_orig.read(), name=orig_name)

    # --- Generate WebP thumbnail ---
    thumb = img.copy()
    thumb.thumbnail((THUMB_WIDTH, THUMB_HEIGHT), PILImage.LANCZOS)

    buf_thumb = io.BytesIO()
    thumb.save(buf_thumb, format='WEBP', quality=THUMB_QUALITY)
    buf_thumb.seek(0)

    thumb_name = f'{uuid.uuid4().hex[:12]}.webp'
    thumbnail = ContentFile(buf_thumb.read(), name=thumb_name)

    return compressed, thumbnail


def generate_thumbnail_for_existing(image_path):
    """
    Generate a WebP thumbnail for an existing image file on disk.
    Returns ContentFile or None if failed.
    """
    try:
        img = PILImage.open(image_path)
        img = ImageOps.exif_transpose(img)

        if img.mode in ('RGBA', 'P', 'LA'):
            background = PILImage.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            background.paste(img, mask=img.split()[-1] if 'A' in img.mode else None)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')

        img.thumbnail((THUMB_WIDTH, THUMB_HEIGHT), PILImage.LANCZOS)

        buf = io.BytesIO()
        img.save(buf, format='WEBP', quality=THUMB_QUALITY)
        buf.seek(0)

        thumb_name = f'{uuid.uuid4().hex[:12]}.webp'
        return ContentFile(buf.read(), name=thumb_name)
    except Exception:
        return None
