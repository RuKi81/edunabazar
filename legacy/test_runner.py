"""
Custom test runner that temporarily sets managed=True on unmanaged models
so that Django creates their tables in the test database.
"""

from django.test.runner import DiscoverRunner


class UnmanagedModelTestRunner(DiscoverRunner):

    def setup_databases(self, **kwargs):
        from django.apps import apps
        self._unmanaged = [m for m in apps.get_models() if not m._meta.managed]
        for m in self._unmanaged:
            m._meta.managed = True
        result = super().setup_databases(**kwargs)
        return result

    def teardown_databases(self, old_config, **kwargs):
        super().teardown_databases(old_config, **kwargs)
        for m in self._unmanaged:
            m._meta.managed = False
