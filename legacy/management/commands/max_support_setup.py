"""
Настройка MAX-бота чата поддержки (dev.max.ru).

  manage.py max_support_setup            — проверить токен (/me), показать чаты,
                                           из которых боту писали (чтобы узнать
                                           MAX_SUPPORT_CHAT_ID), подписать webhook
  manage.py max_support_setup --info     — только показать текущие подписки
  manage.py max_support_setup --delete   — снять подписку на наш webhook
"""
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from legacy import support_chat as svc


class Command(BaseCommand):
    help = 'Проверить MAX-бота поддержки, подсказать chat_id и подписать webhook'

    def add_arguments(self, parser):
        parser.add_argument('--info', action='store_true', help='Только показать GET /subscriptions')
        parser.add_argument('--delete', action='store_true', help='Удалить подписку на наш webhook')

    def handle(self, *args, **options):
        if not settings.MAX_SUPPORT_BOT_TOKEN:
            raise CommandError('MAX_SUPPORT_BOT_TOKEN не задан в .env')

        me = svc.max_call('GET', '/me')
        if me is None:
            raise CommandError('GET /me не прошёл — проверьте токен / доступ к platform-api2.max.ru')
        self.stdout.write(f'Бот: @{me.get("username") or "?"} ({me.get("name", "")}, id {me.get("user_id")})')

        url = settings.SITE_URL.rstrip('/') + svc.max_webhook_path()

        if options['delete']:
            ok = svc.max_call('DELETE', '/subscriptions', params={'url': url})
            self.stdout.write('Подписка удалена' if ok is not None else 'Не удалось удалить подписку')
            return

        if options['info']:
            self._print_subscriptions()
            return

        if not settings.MAX_SUPPORT_CHAT_ID:
            self.stdout.write(self.style.WARNING('MAX_SUPPORT_CHAT_ID не задан. Чаты, писавшие боту недавно:'))
            self._print_recent_chats()
            self.stdout.write('Пропишите MAX_SUPPORT_CHAT_ID в .env и запустите команду снова.')
            return

        result = svc.max_call('POST', '/subscriptions', json={
            'url': url,
            'update_types': ['message_created'],
            'secret': svc.max_webhook_secret(),
        })
        if result is None:
            raise CommandError('POST /subscriptions не прошёл')
        self.stdout.write(self.style.SUCCESS(f'Webhook подписан: {url}'))
        self._print_subscriptions()

    def _print_recent_chats(self):
        # Long polling работает только пока нет активной webhook-подписки.
        data = svc.max_call('GET', '/updates', params={'limit': 100, 'timeout': 0}) or {}
        seen = {}
        for upd in data.get('updates') or []:
            if upd.get('update_type') in ('bot_started', 'bot_added'):
                cid = upd.get('chat_id')
                if cid is not None:
                    seen.setdefault(cid, {'type': upd['update_type'], 'title': (upd.get('user') or {}).get('name', '')})
                continue
            msg = upd.get('message') or {}
            rcp = msg.get('recipient') or {}
            cid = rcp.get('chat_id')
            if cid is not None:
                seen[cid] = {'type': rcp.get('chat_type', ''), 'title': (msg.get('sender') or {}).get('name', '')}
        if not seen:
            self.stdout.write('  (пусто — напишите боту в диалоге или добавьте в группу и отправьте сообщение; '
                              'если уже есть webhook-подписка, снимите её: --delete)')
        for cid, info in seen.items():
            self.stdout.write(f'  chat_id={cid}  type={info["type"]}  {info["title"]}')

    def _print_subscriptions(self):
        data = svc.max_call('GET', '/subscriptions') or {}
        subs = data.get('subscriptions') or []
        if not subs:
            self.stdout.write('  подписок нет')
        for s in subs:
            self.stdout.write(f'  url: {s.get("url")}  types: {s.get("update_types") or "all"}')
