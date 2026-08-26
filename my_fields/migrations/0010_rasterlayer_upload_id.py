from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('my_fields', '0009_rasterlayer'),
    ]

    operations = [
        migrations.AddField(
            model_name='rasterlayer',
            name='upload_id',
            field=models.CharField(blank=True, default='', max_length=255, verbose_name='UploadId (S3 multipart)'),
        ),
    ]
