from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0007_populate_catalog'),
    ]

    operations = [
        migrations.CreateModel(
            name='News',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('title', models.CharField(max_length=500)),
                ('text', models.TextField(blank=True, default='')),
                ('source_url', models.URLField(max_length=1000)),
                ('source_name', models.CharField(blank=True, default='', max_length=200)),
                ('source_title', models.CharField(blank=True, default='', max_length=500)),
                ('published_at', models.DateField()),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('is_active', models.BooleanField(default=True)),
            ],
            options={
                'verbose_name': '\u041d\u043e\u0432\u043e\u0441\u0442\u044c',
                'verbose_name_plural': '\u041d\u043e\u0432\u043e\u0441\u0442\u0438',
                'db_table': 'news',
                'ordering': ['-published_at', '-created_at'],
            },
        ),
    ]
