from django.apps import AppConfig


class MyFieldsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'my_fields'
    verbose_name = 'Мои поля (FMS)'
