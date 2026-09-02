from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('my_fields', '0010_rasterlayer_upload_id'),
    ]

    operations = [
        migrations.AddField(
            model_name='rasterlayer',
            name='is_public',
            field=models.BooleanField(
                default=True,
                help_text='Виден всем пользователям без выдачи гранта.',
                verbose_name='Доступен всем',
            ),
        ),
    ]
