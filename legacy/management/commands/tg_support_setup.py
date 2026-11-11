"""
Настройка Telegram-бота чата поддержки.

  manage.py tg_support_setup            — проверить токен, показать чаты, из
                                          которых боту писали (чтобы узнать
                                          TG_SUPPORT_CHAT_ID), зарегистрировать webhook
  manage.py tg_support_setup --info     — только показать текущее состояние webhook
  manage.py tg_support_setup --delete   — снять webhook
"""
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from legacy import support_chat as svc


class Command(BaseCommand):
    help = 'Проверить бота поддержки, подсказать chat_id и зарегистрировать webhook'

    def add_arguments(self, parser):
        parser.add_argument('--info', action='store_true', help='Только показать getWebhookInfo')
        parser.add_argument('--delete', action='store_true', help='Удалить webhook')

    def handle(self, *args, **options):
        if not settings.TG_SUPPORT_BOT_TOKEN:
            raise CommandError('TG_SUPPORT_BOT_TOKEN не задан в .env')

        me = svc.tg_call('getMe')
        if not me:
            raise CommandError('getMe не прошёл — проверьте токен / доступ к api.telegram.org')
        self.stdout.write(f'Бот: @{me.get("username")} (id {me.get("id")})')

        if options['delete']:
            ok = svc.tg_call('deleteWebhook')
            self.stdout.write('Webhook удалён' if ok is not None else 'Не удалось удалить webhook')
            return

        if options['info']:
            self._print_webhook_info()
            return

        if not settings.TG_SUPPORT_CHAT_ID:
            self.stdout.write(self.style.WARNING('TG_SUPPORT_CHAT_ID не задан. Чаты, писавшие боту недавно:'))
            self._print_recent_chats()
            self.stdout.write('Пропишите TG_SUPPORT_CHAT_ID в .env и запустите команду снова.')
            return

        url = settings.SITE_URL.rstrip('/') + svc.webhook_path()
        result = svc.tg_call(
            'setWebhook', url=url, allowed_updates=['message'],
            drop_pending_updates=False, max_connections=10,
        )
        if result is None:
            raise CommandError('setWebhook не прошёл')
        self.stdout.write(self.style.SUCCESS(f'Webhook установлен: {url}'))
        self._print_webhook_info()

    def _print_recent_chats(self):
        updates = svc.tg_call('getUpdates', timeout=0, allowed_updates=['message']) or []
        seen = {}
        for upd in updates:
            msg = upd.get('message') or {}
            chat = msg.get('chat') or {}
            if chat.get('id') is not None:
                seen[chat['id']] = chat
        if not seen:
            self.stdout.write('  (пусто — напишите боту /start в личке или добавьте в группу и отправьте сообщение)')
        for cid, chat in seen.items():
            title = chat.get('title') or f'{chat.get("first_name", "")} {chat.get("last_name", "")}'.strip()
            self.stdout.write(f'  chat_id={cid}  type={chat.get("type")}  {title}')

    def _print_webhook_info(self):
        info = svc.tg_call('getWebhookInfo') or {}
        self.stdout.write(f'  url: {info.get("url") or "(не установлен)"}')
        self.stdout.write(f'  pending_update_count: {info.get("pending_update_count", 0)}')
        if info.get('last_error_message'):
            self.stdout.write(self.style.WARNING(f'  last_error: {info["last_error_message"]}'))
