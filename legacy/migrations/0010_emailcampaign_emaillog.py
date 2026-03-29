import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0009_newskeyword_newsfeedsource'),
    ]

    operations = [
        migrations.CreateModel(
            name='EmailCampaign',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255, verbose_name='Название кампании')),
                ('subject', models.CharField(max_length=255, verbose_name='Тема письма')),
                ('body_html', models.TextField(verbose_name='HTML-тело письма')),
                ('body_text', models.TextField(blank=True, default='', verbose_name='Текстовое тело письма')),
                ('from_email', models.CharField(blank=True, default='', max_length=255, verbose_name='Адрес отправителя')),
                ('audience', models.CharField(choices=[('all', 'Все пользователи'), ('imported', 'Импортированные'), ('registered', 'Зарегистрированные на сайте')], default='all', max_length=20, verbose_name='Аудитория')),
                ('status', models.CharField(choices=[('draft', 'Черновик'), ('sending', 'Отправляется'), ('paused', 'Приостановлена'), ('done', 'Завершена')], default='draft', max_length=10, verbose_name='Статус')),
                ('total_recipients', models.PositiveIntegerField(default=0, verbose_name='Всего получателей')),
                ('sent_count', models.PositiveIntegerField(default=0, verbose_name='Отправлено')),
                ('failed_count', models.PositiveIntegerField(default=0, verbose_name='Ошибок')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('started_at', models.DateTimeField(blank=True, null=True, verbose_name='Начало отправки')),
                ('finished_at', models.DateTimeField(blank=True, null=True, verbose_name='Завершение')),
            ],
            options={
                'verbose_name': 'Email-кампания',
                'verbose_name_plural': 'Email-кампании',
                'db_table': 'email_campaign',
                'ordering': ['-created_at'],
            },
        ),
        migrations.CreateModel(
            name='EmailLog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('recipient_email', models.EmailField(max_length=254, verbose_name='Email получателя')),
                ('status', models.CharField(choices=[('pending', 'Ожидает'), ('sent', 'Отправлено'), ('failed', 'Ошибка')], default='pending', max_length=10, verbose_name='Статус')),
                ('error_message', models.TextField(blank=True, default='', verbose_name='Сообщение об ошибке')),
                ('sent_at', models.DateTimeField(blank=True, null=True, verbose_name='Время отправки')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('campaign', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='logs', to='legacy.emailcampaign')),
            ],
            options={
                'verbose_name': 'Email-лог',
                'verbose_name_plural': 'Email-логи',
                'db_table': 'email_log',
                'ordering': ['-created_at'],
            },
        ),
        migrations.AddIndex(
            model_name='emaillog',
            index=models.Index(fields=['campaign', 'status'], name='email_log_campaig_b1c2e3_idx'),
        ),
        migrations.AddIndex(
            model_name='emaillog',
            index=models.Index(fields=['recipient_email'], name='email_log_recipie_a4d5f6_idx'),
        ),
    ]
