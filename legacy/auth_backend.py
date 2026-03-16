import logging

from django.contrib.auth import get_user_model
from django.contrib.auth.backends import BaseBackend
from django.contrib.auth.hashers import check_password

from .models import LegacyUser

logger = logging.getLogger(__name__)
User = get_user_model()


class LegacyUserBackend(BaseBackend):
    """
    Authenticate against legacy_user table.
    On success, find or create a matching Django auth.User
    so the same credentials work for both site and /admin/.
    """

    def authenticate(self, request, username=None, password=None, **kwargs):
        if not username or not password:
            return None

        legacy_user = LegacyUser.objects.filter(username=username).first()
        if legacy_user is None:
            return None

        if not check_password(password, legacy_user.password_hash or ''):
            return None

        django_user = self._get_or_create_django_user(legacy_user)
        return django_user

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None

    @staticmethod
    def _get_or_create_django_user(legacy_user):
        """Find or create a Django auth.User linked to the LegacyUser."""
        django_user = User.objects.filter(username=legacy_user.username).first()

        if django_user is None:
            django_user = User(
                username=legacy_user.username,
                email=legacy_user.email or '',
                first_name=(legacy_user.name or '')[:30],
                is_active=True,
                is_staff=False,
                is_superuser=False,
            )
            django_user.set_unusable_password()
            django_user.save()

        django_user._legacy_user = legacy_user
        return django_user
