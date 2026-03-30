from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0013_advert_search_vector'),
    ]

    operations = [
        migrations.CreateModel(
            name='Favorite',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='favorites', to='legacy.legacyuser')),
                ('advert', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='favorites', to='legacy.advert')),
            ],
            options={
                'db_table': 'favorite',
                'unique_together': {('user', 'advert')},
            },
        ),
        migrations.CreateModel(
            name='AdvertView',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ip_address', models.GenericIPAddressField()),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('advert', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='views', to='legacy.advert')),
                ('user', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='legacy.legacyuser')),
            ],
            options={
                'db_table': 'advert_view',
            },
        ),
        migrations.AddIndex(
            model_name='favorite',
            index=models.Index(fields=['user', '-created_at'], name='favorite_user_id_created_idx'),
        ),
        migrations.AddIndex(
            model_name='advertview',
            index=models.Index(fields=['advert', '-created_at'], name='advert_view_advert_created_idx'),
        ),
        migrations.AddIndex(
            model_name='advertview',
            index=models.Index(fields=['advert', 'ip_address'], name='advert_view_advert_ip_idx'),
        ),
    ]
