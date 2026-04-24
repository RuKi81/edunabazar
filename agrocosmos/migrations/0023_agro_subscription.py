from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0022_vegetation_alert'),
    ]

    operations = [
        migrations.CreateModel(
            name='AgroSubscription',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('legacy_user_id', models.IntegerField(verbose_name='Пользователь (legacy_user.id)')),
                ('notify_anomalies', models.BooleanField(default=True, verbose_name='Уведомлять об аномалиях')),
                ('notify_updates', models.BooleanField(default=False, verbose_name='Получать уведомления об обновлениях')),
                ('last_update_notified_at', models.DateTimeField(blank=True, null=True,
                    verbose_name='Когда последний раз отправлен дайджест обновлений')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('district', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=models.deletion.CASCADE,
                    related_name='agro_subscriptions',
                    to='agrocosmos.district',
                    verbose_name='Район')),
                ('region', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=models.deletion.CASCADE,
                    related_name='agro_subscriptions',
                    to='agrocosmos.region',
                    verbose_name='Субъект')),
            ],
            options={
                'verbose_name': 'Подписка на уведомления',
                'verbose_name_plural': 'Подписки на уведомления',
                'db_table': 'agro_subscription',
                'ordering': ['region__name', 'district__name'],
            },
        ),
        migrations.AddConstraint(
            model_name='agrosubscription',
            constraint=models.CheckConstraint(
                condition=models.Q(('region__isnull', False)) | models.Q(('district__isnull', False)),
                name='agrosub_scope_required',
            ),
        ),
        migrations.AddConstraint(
            model_name='agrosubscription',
            constraint=models.UniqueConstraint(
                fields=('legacy_user_id', 'region', 'district'),
                name='agrosub_unique_scope',
            ),
        ),
        migrations.AddIndex(
            model_name='agrosubscription',
            index=models.Index(fields=['legacy_user_id'], name='agro_subscr_legacy__a8b0df_idx'),
        ),
        migrations.AddIndex(
            model_name='agrosubscription',
            index=models.Index(fields=['region', 'district'], name='agro_subscr_region__b8a5b6_idx'),
        ),
    ]
