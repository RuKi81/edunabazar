"""Тесты собственного чата поддержки (виджет ↔ бот MAX / Telegram)."""
import json
from unittest import mock

from django.core.management import call_command
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from . import support_chat as svc
from .constants import USER_STATUS_ACTIVE
from .models import LegacyUser, SupportMessage, SupportThread

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}
TG = dict(TG_SUPPORT_BOT_TOKEN='123:ABC', TG_SUPPORT_CHAT_ID='-100500', SITE_URL='https://edunabazar.ru')
MAX = dict(MAX_SUPPORT_BOT_TOKEN='max-token', MAX_SUPPORT_CHAT_ID='-7001', SITE_URL='https://edunabazar.ru',
           TG_SUPPORT_BOT_TOKEN='', TG_SUPPORT_CHAT_ID='')


def _make_user(username, **overrides):
    now = timezone.now()
    kwargs = dict(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RU', name='Иван', address='',
        phone='', inn='', status=USER_STATUS_ACTIVE,
        created_at=now, updated_at=now, contacts='',
    )
    kwargs.update(overrides)
    return LegacyUser.objects.create(**kwargs)


def _login(client, user):
    client.get('/about/')
    session = client.session
    session['legacy_user_id'] = user.pk
    session.save()
    from django.conf import settings as _s
    client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key
    return client


def _send(client, text, **extra):
    return client.post(
        '/api/support/send/', data=json.dumps({'text': text, 'page': 'https://edunabazar.ru/adverts/5/'}),
        content_type='application/json', HTTP_SEC_FETCH_SITE='same-origin', **extra,
    )


def _tg_ok(message_id=777):
    return mock.patch('legacy.support_chat.tg_call', return_value={'message_id': message_id})


@override_settings(CACHES=_DUMMY_CACHE, **TG)
class SendAndPollTests(TestCase):

    def test_first_message_creates_thread_and_cookie(self):
        client = Client()
        with _tg_ok() as tg:
            resp = _send(client, 'Здравствуйте!')
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertEqual(data['message']['dir'], 'in')
        self.assertIn(svc.VISITOR_COOKIE, resp.cookies)
        self.assertTrue(resp.cookies[svc.VISITOR_COOKIE]['httponly'])

        thread = SupportThread.objects.get()
        self.assertEqual(thread.page_url, 'https://edunabazar.ru/adverts/5/')
        self.assertIsNone(thread.user)
        msg = thread.messages.get()
        self.assertEqual(msg.tg_message_id, 777)

        tg.assert_called_once()
        kwargs = tg.call_args.kwargs
        self.assertEqual(kwargs['chat_id'], '-100500')
        self.assertIn(f'Обращение #{thread.id}', kwargs['text'])
        self.assertIn('Здравствуйте!', kwargs['text'])

    def test_second_message_reuses_thread(self):
        client = Client()
        with _tg_ok():
            _send(client, 'один')
            _send(client, 'два')
        self.assertEqual(SupportThread.objects.count(), 1)
        self.assertEqual(SupportMessage.objects.count(), 2)

    def test_logged_in_user_linked_to_thread(self):
        user = _make_user('ivan')
        client = _login(Client(), user)
        with _tg_ok() as tg:
            _send(client, 'вопрос')
        self.assertEqual(SupportThread.objects.get().user_id, user.pk)
        self.assertIn('ivan', tg.call_args.kwargs['text'])

    def test_empty_and_too_long_rejected(self):
        client = Client()
        with _tg_ok():
            self.assertEqual(_send(client, '   ').status_code, 400)
            self.assertEqual(_send(client, 'x' * (svc.MAX_TEXT_LEN + 1)).status_code, 400)
        self.assertEqual(SupportMessage.objects.count(), 0)

    def test_cross_site_post_forbidden(self):
        resp = Client().post(
            '/api/support/send/', data=json.dumps({'text': 'hi'}),
            content_type='application/json', HTTP_SEC_FETCH_SITE='cross-site',
        )
        self.assertEqual(resp.status_code, 403)

    def test_rate_limit(self):
        client = Client()
        with _tg_ok(), mock.patch('legacy.support_chat.RATE_LIMIT_COUNT', 2):
            _send(client, '1')
            _send(client, '2')
            resp = _send(client, '3')
        self.assertEqual(resp.status_code, 429)
        self.assertEqual(SupportMessage.objects.count(), 2)

    def test_message_kept_when_telegram_not_configured(self):
        client = Client()
        with override_settings(TG_SUPPORT_BOT_TOKEN='', TG_SUPPORT_CHAT_ID=''), \
                mock.patch('legacy.support_chat.tg_call') as tg:
            resp = _send(client, 'без бота')
        self.assertEqual(resp.status_code, 200)
        tg.assert_not_called()
        self.assertIsNone(SupportMessage.objects.get().tg_message_id)

    def test_poll_returns_only_new_messages(self):
        client = Client()
        with _tg_ok():
            first = _send(client, 'первое').json()['message']['id']
            _send(client, 'второе')
        thread = SupportThread.objects.get()
        svc.add_operator_message(thread, 'ответ')

        resp = client.get(f'/api/support/messages/?after={first}')
        msgs = resp.json()['messages']
        self.assertEqual([m['text'] for m in msgs], ['второе', 'ответ'])
        self.assertEqual(msgs[1]['dir'], 'out')

    def test_poll_without_cookie_is_empty(self):
        resp = Client().get('/api/support/messages/')
        self.assertEqual(resp.json(), {'ok': True, 'messages': []})

    def test_poll_with_foreign_cookie_is_empty(self):
        client = Client()
        with _tg_ok():
            _send(client, 'секрет')
        other = Client()
        other.cookies[svc.VISITOR_COOKIE] = 'deadbeef'
        self.assertEqual(other.get('/api/support/messages/').json()['messages'], [])


@override_settings(CACHES=_DUMMY_CACHE, **TG)
class TelegramWebhookTests(TestCase):

    def setUp(self):
        self.thread = SupportThread.objects.create(visitor_key='k1')
        self.incoming = SupportMessage.objects.create(
            thread=self.thread, direction=SupportMessage.DIRECTION_IN, text='вопрос', tg_message_id=777,
        )
        self.url = svc.webhook_path()

    def _update(self, text, reply_to=None, chat_id='-100500', message_id=900):
        msg = {'message_id': message_id, 'chat': {'id': int(chat_id)}, 'text': text}
        if reply_to is not None:
            msg['reply_to_message'] = reply_to
        return {'update_id': 1, 'message': msg}

    def _post(self, update, url=None):
        return Client().post(url or self.url, data=json.dumps(update), content_type='application/json')

    def test_reply_to_forwarded_message_creates_operator_message(self):
        with mock.patch('legacy.support_chat.tg_call') as tg:
            resp = self._post(self._update('Ответ оператора', reply_to={'message_id': 777, 'text': 'x'}))
        self.assertEqual(resp.status_code, 200)
        out = self.thread.messages.get(direction=SupportMessage.DIRECTION_OUT)
        self.assertEqual(out.text, 'Ответ оператора')
        self.assertEqual(out.tg_message_id, 900)
        self.thread.refresh_from_db()
        self.assertFalse(self.thread.awaiting_reply)
        tg.assert_not_called()

    def test_reply_resolved_by_thread_number_in_quoted_text(self):
        reply = {'message_id': 1, 'text': f'💬 Обращение #{self.thread.id} — Гость'}
        with mock.patch('legacy.support_chat.tg_call'):
            self._post(self._update('через номер', reply_to=reply))
        self.assertTrue(self.thread.messages.filter(direction='out', text='через номер').exists())

    def test_non_reply_message_gets_hint(self):
        with mock.patch('legacy.support_chat.tg_call') as tg:
            self._post(self._update('просто текст'))
        self.assertEqual(SupportMessage.objects.filter(direction='out').count(), 0)
        tg.assert_called_once()
        self.assertIn('Reply', tg.call_args.kwargs['text'])

    def test_foreign_chat_ignored(self):
        with mock.patch('legacy.support_chat.tg_call') as tg:
            self._post(self._update('чужой', reply_to={'message_id': 777}, chat_id='42'))
        self.assertEqual(SupportMessage.objects.filter(direction='out').count(), 0)
        tg.assert_not_called()

    def test_duplicate_update_ignored(self):
        upd = self._update('дубль', reply_to={'message_id': 777})
        with mock.patch('legacy.support_chat.tg_call'):
            self._post(upd)
            self._post(upd)
        self.assertEqual(SupportMessage.objects.filter(direction='out').count(), 1)

    def test_wrong_secret_404(self):
        resp = self._post(self._update('x'), url='/api/support/tg/wrong/')
        self.assertEqual(resp.status_code, 404)

    def test_secret_derived_from_secret_key_when_not_set(self):
        with override_settings(TG_SUPPORT_WEBHOOK_SECRET=''):
            derived = svc.webhook_secret()
        with override_settings(TG_SUPPORT_WEBHOOK_SECRET='explicit'):
            self.assertEqual(svc.webhook_secret(), 'explicit')
        self.assertEqual(len(derived), 40)


@override_settings(CACHES=_DUMMY_CACHE, **TG)
class AdminSupportTests(TestCase):

    def setUp(self):
        self.client_ = _login(Client(), _make_user('admin'))
        self.thread = SupportThread.objects.create(visitor_key='k1', page_url='https://edunabazar.ru/')
        SupportMessage.objects.create(thread=self.thread, direction='in', text='помогите')
        self.thread.last_visitor_message_at = timezone.now()
        self.thread.save()

    def test_anonymous_redirected(self):
        resp = Client().get('/legacy-admin/support/')
        self.assertEqual(resp.status_code, 302)

    def test_list_shows_thread_awaiting_reply(self):
        resp = self.client_.get('/legacy-admin/support/')
        self.assertContains(resp, f'#{self.thread.pk}')
        self.assertContains(resp, 'Ждёт ответа')

    def test_detail_and_reply_from_admin(self):
        resp = self.client_.get(f'/legacy-admin/support/{self.thread.pk}/')
        self.assertContains(resp, 'помогите')
        resp = self.client_.post(f'/legacy-admin/support/{self.thread.pk}/', {'text': 'уже помогаем'})
        self.assertEqual(resp.status_code, 302)
        out = self.thread.messages.get(direction='out')
        self.assertEqual(out.text, 'уже помогаем')
        self.thread.refresh_from_db()
        self.assertFalse(self.thread.awaiting_reply)

    def test_admin_nav_has_support_link(self):
        resp = self.client_.get('/legacy-admin/')
        self.assertContains(resp, '/legacy-admin/support/')


@override_settings(CACHES=_DUMMY_CACHE, **MAX)
class MaxNotifyTests(TestCase):

    def test_visitor_message_forwarded_to_max_and_mid_saved(self):
        client = Client()
        with mock.patch('legacy.support_chat.max_call',
                        return_value={'message': {'body': {'mid': 'mid.123'}}}) as mx, \
                mock.patch('legacy.support_chat.tg_call') as tg:
            resp = _send(client, 'Здравствуйте!')
        self.assertEqual(resp.status_code, 200)
        tg.assert_not_called()
        mx.assert_called_once()
        args, kwargs = mx.call_args
        self.assertEqual(args, ('POST', '/messages'))
        self.assertEqual(kwargs['params'], {'chat_id': '-7001'})
        thread = SupportThread.objects.get()
        self.assertIn(f'Обращение #{thread.id}', kwargs['json']['text'])
        self.assertIn('Здравствуйте!', kwargs['json']['text'])
        msg = thread.messages.get()
        self.assertEqual(msg.max_message_id, 'mid.123')
        self.assertIsNone(msg.tg_message_id)

    @override_settings(TG_SUPPORT_BOT_TOKEN='123:ABC', TG_SUPPORT_CHAT_ID='-100500')
    def test_both_transports_notified(self):
        with mock.patch('legacy.support_chat.max_call',
                        return_value={'message': {'body': {'mid': 'm1'}}}) as mx, \
                _tg_ok(555) as tg:
            _send(Client(), 'оба')
        mx.assert_called_once()
        tg.assert_called_once()
        msg = SupportMessage.objects.get()
        self.assertEqual((msg.max_message_id, msg.tg_message_id), ('m1', 555))

    def test_max_api_failure_keeps_message(self):
        with mock.patch('legacy.support_chat.max_call', return_value=None):
            resp = _send(Client(), 'без связи')
        self.assertEqual(resp.status_code, 200)
        self.assertIsNone(SupportMessage.objects.get().max_message_id)


@override_settings(CACHES=_DUMMY_CACHE, **MAX)
class MaxWebhookTests(TestCase):

    def setUp(self):
        self.thread = SupportThread.objects.create(visitor_key='k1')
        self.incoming = SupportMessage.objects.create(
            thread=self.thread, direction=SupportMessage.DIRECTION_IN, text='вопрос', max_message_id='mid.in',
        )
        self.url = svc.max_webhook_path()

    def _update(self, text, reply_mid=None, reply_text='', chat_id='-7001', mid='mid.out',
                update_type='message_created', is_bot=False):
        message = {
            'sender': {'user_id': 1, 'name': 'Оператор', 'is_bot': is_bot},
            'recipient': {'chat_id': int(chat_id), 'chat_type': 'chat'},
            'body': {'mid': mid, 'seq': 1, 'text': text},
        }
        if reply_mid is not None or reply_text:
            message['link'] = {'type': 'reply', 'message': {'mid': reply_mid, 'text': reply_text}}
        return {'update_type': update_type, 'timestamp': 0, 'message': message}

    def _post(self, update, url=None, **extra):
        return Client().post(url or self.url, data=json.dumps(update), content_type='application/json', **extra)

    def test_reply_to_forwarded_message_creates_operator_message(self):
        with mock.patch('legacy.support_chat.max_call') as mx:
            resp = self._post(self._update('Ответ оператора', reply_mid='mid.in'))
        self.assertEqual(resp.status_code, 200)
        out = self.thread.messages.get(direction=SupportMessage.DIRECTION_OUT)
        self.assertEqual(out.text, 'Ответ оператора')
        self.assertEqual(out.max_message_id, 'mid.out')
        self.thread.refresh_from_db()
        self.assertFalse(self.thread.awaiting_reply)
        mx.assert_not_called()

    def test_reply_resolved_by_thread_number_in_quoted_text(self):
        with mock.patch('legacy.support_chat.max_call'):
            self._post(self._update('через номер', reply_mid='unknown',
                                    reply_text=f'💬 Обращение #{self.thread.id} — Гость'))
        self.assertTrue(self.thread.messages.filter(direction='out', text='через номер').exists())

    def test_non_reply_message_gets_hint(self):
        with mock.patch('legacy.support_chat.max_call') as mx:
            self._post(self._update('просто текст'))
        self.assertEqual(SupportMessage.objects.filter(direction='out').count(), 0)
        mx.assert_called_once()
        args, kwargs = mx.call_args
        self.assertEqual(args, ('POST', '/messages'))
        self.assertIn('Reply', kwargs['json']['text'])
        self.assertEqual(kwargs['json']['link'], {'type': 'reply', 'mid': 'mid.out'})

    def test_foreign_chat_ignored(self):
        with mock.patch('legacy.support_chat.max_call') as mx:
            self._post(self._update('чужой', reply_mid='mid.in', chat_id='42'))
        self.assertEqual(SupportMessage.objects.filter(direction='out').count(), 0)
        mx.assert_not_called()

    def test_bot_messages_and_other_update_types_ignored(self):
        with mock.patch('legacy.support_chat.max_call') as mx:
            self._post(self._update('эхо', reply_mid='mid.in', is_bot=True))
            self._post(self._update('старт', reply_mid='mid.in', update_type='bot_started'))
        self.assertEqual(SupportMessage.objects.filter(direction='out').count(), 0)
        mx.assert_not_called()

    def test_duplicate_update_ignored(self):
        upd = self._update('дубль', reply_mid='mid.in')
        with mock.patch('legacy.support_chat.max_call'):
            self._post(upd)
            self._post(upd)
        self.assertEqual(SupportMessage.objects.filter(direction='out').count(), 1)

    def test_wrong_secret_404(self):
        resp = self._post(self._update('x'), url='/api/support/max/wrong/')
        self.assertEqual(resp.status_code, 404)

    def test_wrong_secret_header_404(self):
        resp = self._post(self._update('x', reply_mid='mid.in'), HTTP_X_MAX_BOT_API_SECRET='bad')
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(SupportMessage.objects.filter(direction='out').count(), 0)

    def test_correct_secret_header_accepted(self):
        with mock.patch('legacy.support_chat.max_call'):
            resp = self._post(self._update('x', reply_mid='mid.in'),
                              HTTP_X_MAX_BOT_API_SECRET=svc.max_webhook_secret())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(SupportMessage.objects.filter(direction='out').count(), 1)

    def test_secret_derived_from_secret_key_when_not_set(self):
        with override_settings(MAX_SUPPORT_WEBHOOK_SECRET=''):
            derived = svc.max_webhook_secret()
        with override_settings(MAX_SUPPORT_WEBHOOK_SECRET='explicit'):
            self.assertEqual(svc.max_webhook_secret(), 'explicit')
        self.assertEqual(len(derived), 40)
        self.assertNotEqual(derived, svc.webhook_secret())


@override_settings(**MAX)
class MaxSetupCommandTests(TestCase):

    def test_subscribes_webhook_with_site_url_and_secret(self):
        calls = []

        def fake(method, path, **kw):
            calls.append((method, path, kw))
            if path == '/me':
                return {'username': 'enb_support_bot', 'name': 'Поддержка', 'user_id': 1}
            if path == '/subscriptions' and method == 'GET':
                return {'subscriptions': [{'url': 'x', 'update_types': ['message_created']}]}
            return {'success': True}

        with mock.patch('legacy.support_chat.max_call', side_effect=fake):
            call_command('max_support_setup', verbosity=0)
        post = [c for c in calls if c[0] == 'POST' and c[1] == '/subscriptions']
        self.assertEqual(len(post), 1)
        body = post[0][2]['json']
        self.assertEqual(body['url'], 'https://edunabazar.ru' + svc.max_webhook_path())
        self.assertEqual(body['secret'], svc.max_webhook_secret())
        self.assertEqual(body['update_types'], ['message_created'])

    @override_settings(MAX_SUPPORT_CHAT_ID='')
    def test_lists_chats_when_chat_id_missing(self):
        import io

        def fake(method, path, **kw):
            if path == '/me':
                return {'username': 'bot', 'user_id': 1}
            if path == '/updates':
                return {'updates': [
                    {'update_type': 'bot_added', 'chat_id': -42, 'user': {'name': 'Админ'}},
                    {'update_type': 'message_created', 'message': {
                        'sender': {'name': 'Иван'}, 'recipient': {'chat_id': 77, 'chat_type': 'dialog'},
                        'body': {'text': 'hi'}}},
                ]}
            return {}

        out = io.StringIO()
        with mock.patch('legacy.support_chat.max_call', side_effect=fake) as mx:
            call_command('max_support_setup', stdout=out)
        self.assertIn('chat_id=-42', out.getvalue())
        self.assertIn('chat_id=77', out.getvalue())
        self.assertIn('Иван', out.getvalue())
        self.assertFalse(any(c.args[0] == 'POST' for c in mx.call_args_list))

    def test_delete_subscription(self):
        with mock.patch('legacy.support_chat.max_call', return_value={'success': True}) as mx:
            call_command('max_support_setup', '--delete', verbosity=0)
        delete = [c for c in mx.call_args_list if c.args[0] == 'DELETE']
        self.assertEqual(len(delete), 1)
        self.assertEqual(delete[0].kwargs['params']['url'], 'https://edunabazar.ru' + svc.max_webhook_path())


class CaBundleTests(TestCase):

    def test_bundle_contains_certifi_and_ru_certs(self):
        import certifi
        svc._ca_bundle_path = None
        path = svc.ca_bundle()
        data = open(path, 'rb').read()
        self.assertTrue(data.startswith(open(certifi.where(), 'rb').read()[:200]))
        for pem in sorted(svc._RU_CERTS_DIR.glob('*.pem')):
            self.assertIn(pem.read_bytes().strip(), data)
        self.assertEqual(svc.ca_bundle(), path)


@override_settings(**TG)
class SetupCommandTests(TestCase):

    def test_sets_webhook_with_site_url_and_secret(self):
        calls = {}

        def fake(method, **params):
            calls[method] = params
            return {'username': 'bot', 'id': 1} if method == 'getMe' else {'url': 'x'}

        with mock.patch('legacy.support_chat.tg_call', side_effect=fake):
            call_command('tg_support_setup', verbosity=0)
        self.assertEqual(calls['setWebhook']['url'], 'https://edunabazar.ru' + svc.webhook_path())

    @override_settings(TG_SUPPORT_CHAT_ID='')
    def test_lists_chats_when_chat_id_missing(self):
        import io

        def fake(method, **params):
            if method == 'getMe':
                return {'username': 'bot', 'id': 1}
            if method == 'getUpdates':
                return [{'message': {'chat': {'id': -42, 'type': 'group', 'title': 'Поддержка'}}}]
            return {}

        out = io.StringIO()
        with mock.patch('legacy.support_chat.tg_call', side_effect=fake):
            call_command('tg_support_setup', stdout=out)
        self.assertIn('chat_id=-42', out.getvalue())
        self.assertIn('Поддержка', out.getvalue())
