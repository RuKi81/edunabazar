"""
Тесты отзывов и сообщений (``legacy/views/reviews.py``,
``legacy/views/messages.py``) — страховочная сетка перед рефакторингом
``review_create`` (C=16) и ``message_send`` (C=15).

Покрывается: создание отзыва (тип advert/seller, модерация, дубликаты,
клампинг оценки, обрезка текста), модерация отзывов (author/admin),
отправка сообщений (валидация получателя, привязка объявления,
email-уведомление), inbox/thread/unread API.
"""
from unittest import mock

from django.test import Client, TestCase, override_settings
from django.utils import timezone

from .constants import (
    REVIEW_STATUS_DELETED, REVIEW_STATUS_HIDDEN, REVIEW_STATUS_MODERATION,
    REVIEW_STATUS_PUBLISHED, USER_STATUS_ACTIVE,
)
from .models import LegacyUser, Message, Review

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}


def _make_user(username, **overrides):
    now = timezone.now()
    kwargs = dict(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RU', name='', address='',
        phone='', inn='', status=USER_STATUS_ACTIVE,
        created_at=now, updated_at=now, contacts='',
    )
    kwargs.update(overrides)
    return LegacyUser.objects.create(**kwargs)


def _login(client, user):
    client.get('/')
    session = client.session
    session['legacy_user_id'] = user.pk
    session.save()
    from django.conf import settings as _s
    client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key


def _client_for(user):
    client = Client()
    _login(client, user)
    return client


def _make_review(author, object_id=1, review_type=0,
                 status=REVIEW_STATUS_PUBLISHED, **overrides):
    now = timezone.now()
    kwargs = dict(
        type=review_type, object_id=object_id, points=5,
        author_id=author.pk, text='Отличный товар',
        created_at=now, updated_at=now, status=status,
    )
    kwargs.update(overrides)
    return Review.objects.create(**kwargs)


@override_settings(CACHES=_DUMMY_CACHE)
class ReviewCreateTests(TestCase):
    URL = '/reviews/add/'

    def setUp(self):
        self.user = _make_user('revuser')
        self.client_ = _client_for(self.user)

    def _post(self, **data):
        base = {
            'review_type': '0',
            'object_id': '42',
            'points': '4',
            'text': 'Хороший продавец',
        }
        base.update(data)
        return self.client_.post(self.URL, base)

    def test_get_redirects(self):
        resp = self.client_.get(self.URL)
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], '/adverts/')

    def test_anonymous_redirected_to_login(self):
        resp = Client().post(self.URL, {'object_id': '42', 'text': 'x'})
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(resp['Location'].startswith('/login/'))

    def test_creates_review_on_moderation(self):
        resp = self._post()
        self.assertEqual(resp['Location'], '/adverts/42/')
        review = Review.objects.get(object_id=42, author_id=self.user.pk)
        self.assertEqual(review.status, REVIEW_STATUS_MODERATION)
        self.assertEqual(review.points, 4)
        self.assertEqual(
            self.client_.session['review_success'], 'Отзыв отправлен на модерацию',
        )

    def test_seller_review_redirects_to_seller(self):
        resp = self._post(review_type='1')
        self.assertEqual(resp['Location'], '/sellers/42/')
        self.assertTrue(Review.objects.filter(type=1, object_id=42).exists())

    def test_invalid_review_type_falls_back_to_advert(self):
        self._post(review_type='99')
        self.assertTrue(Review.objects.filter(type=0, object_id=42).exists())

    def test_invalid_object_id_redirects(self):
        resp = self._post(object_id='garbage')
        self.assertEqual(resp['Location'], '/adverts/')
        self.assertFalse(Review.objects.exists())

    def test_points_clamped_and_defaulted(self):
        self._post(points='99')
        self.assertEqual(Review.objects.get(object_id=42).points, 5)
        Review.objects.all().delete()
        self._post(points='garbage')
        self.assertEqual(Review.objects.get(object_id=42).points, 5)
        Review.objects.all().delete()
        self._post(points='0')
        self.assertEqual(Review.objects.get(object_id=42).points, 1)

    def test_empty_text_sets_error(self):
        resp = self._post(text='')
        self.assertEqual(resp['Location'], '/adverts/42/')
        self.assertEqual(self.client_.session['review_error'], 'Введите текст отзыва')
        self.assertFalse(Review.objects.exists())

    def test_long_text_truncated(self):
        self._post(text='ы' * 3000)
        self.assertEqual(len(Review.objects.get(object_id=42).text), 2000)

    def test_duplicate_review_rejected(self):
        _make_review(self.user, object_id=42)
        resp = self._post()
        self.assertEqual(self.client_.session['review_error'], 'Вы уже оставили отзыв')
        self.assertEqual(resp['Location'], '/adverts/42/')
        self.assertEqual(Review.objects.count(), 1)

    def test_deleted_review_does_not_block_new_one(self):
        _make_review(self.user, object_id=42, status=REVIEW_STATUS_DELETED)
        self._post()
        self.assertEqual(
            Review.objects.filter(status=REVIEW_STATUS_MODERATION).count(), 1,
        )


@override_settings(CACHES=_DUMMY_CACHE)
class ReviewModerationTests(TestCase):
    def setUp(self):
        self.author = _make_user('revauthor')
        self.stranger = _make_user('revstranger')
        self.admin = _make_user('admin')
        self.review = _make_review(self.author, object_id=7)

    def _post(self, user, path, **data):
        return _client_for(user).post(path, data)

    def test_author_can_delete(self):
        self._post(self.author, f'/reviews/{self.review.pk}/delete/')
        self.review.refresh_from_db()
        self.assertEqual(self.review.status, REVIEW_STATUS_DELETED)

    def test_stranger_cannot_delete(self):
        self._post(self.stranger, f'/reviews/{self.review.pk}/delete/')
        self.review.refresh_from_db()
        self.assertEqual(self.review.status, REVIEW_STATUS_PUBLISHED)

    def test_admin_can_hide_and_publish(self):
        self._post(self.admin, f'/reviews/{self.review.pk}/hide/')
        self.review.refresh_from_db()
        self.assertEqual(self.review.status, REVIEW_STATUS_HIDDEN)
        self._post(self.admin, f'/reviews/{self.review.pk}/publish/')
        self.review.refresh_from_db()
        self.assertEqual(self.review.status, REVIEW_STATUS_PUBLISHED)

    def test_non_admin_cannot_publish(self):
        self.review.status = REVIEW_STATUS_MODERATION
        self.review.save(update_fields=['status'])
        self._post(self.stranger, f'/reviews/{self.review.pk}/publish/')
        self.review.refresh_from_db()
        self.assertEqual(self.review.status, REVIEW_STATUS_MODERATION)

    def test_delete_safe_next_redirect(self):
        resp = self._post(
            self.author, f'/reviews/{self.review.pk}/delete/', next='/adverts/7/',
        )
        self.assertEqual(resp['Location'], '/adverts/7/')
        resp2 = _client_for(self.admin).post(
            f'/reviews/{self.review.pk}/publish/',
            {'next': 'https://evil.example.com/'},
        )
        self.assertEqual(resp2['Location'], '/adverts/')


@override_settings(CACHES=_DUMMY_CACHE)
class MessageSendTests(TestCase):
    URL = '/messages/send/'

    def setUp(self):
        self.sender = _make_user('msgsender', name='Иван')
        self.recipient = _make_user('msgrecipient')
        self.client_ = _client_for(self.sender)

    def _post(self, **data):
        base = {
            'recipient_id': str(self.recipient.pk),
            'text': 'Здравствуйте, актуально?',
        }
        base.update(data)
        return self.client_.post(self.URL, base)

    def test_get_redirects(self):
        resp = self.client_.get(self.URL)
        self.assertEqual(resp['Location'], '/messages/')

    def test_anonymous_redirected_to_login(self):
        resp = Client().post(self.URL, {'recipient_id': '1', 'text': 'x'})
        self.assertEqual(resp['Location'], '/login/')

    @mock.patch('legacy.views.messages._send_new_message_email', return_value=True)
    def test_valid_send_creates_message_and_notifies(self, mock_email):
        resp = self._post()
        self.assertEqual(resp['Location'], f'/messages/{self.recipient.pk}/')
        msg = Message.objects.get()
        self.assertEqual(msg.sender_id, self.sender.pk)
        self.assertEqual(msg.recipient_id, self.recipient.pk)
        self.assertFalse(msg.is_read)
        mock_email.assert_called_once()
        self.assertEqual(mock_email.call_args[0][0], 'msgrecipient@test.com')
        self.assertEqual(mock_email.call_args[0][1], 'Иван')

    def test_send_to_self_rejected(self):
        resp = self._post(recipient_id=str(self.sender.pk))
        self.assertEqual(resp['Location'], '/messages/')
        self.assertFalse(Message.objects.exists())

    def test_unknown_recipient_rejected(self):
        resp = self._post(recipient_id='999999')
        self.assertEqual(resp['Location'], '/messages/')
        self.assertFalse(Message.objects.exists())

    def test_garbage_recipient_rejected(self):
        resp = self._post(recipient_id='garbage')
        self.assertEqual(resp['Location'], '/messages/')
        self.assertFalse(Message.objects.exists())

    def test_empty_text_rejected(self):
        resp = self._post(text='')
        self.assertEqual(resp['Location'], f'/messages/{self.recipient.pk}/')
        self.assertFalse(Message.objects.exists())

    @mock.patch('legacy.views.messages._send_new_message_email', return_value=True)
    def test_long_text_truncated(self, _):
        self._post(text='ы' * 6000)
        self.assertEqual(len(Message.objects.get().text), 5000)

    @mock.patch('legacy.views.messages._send_new_message_email', return_value=True)
    def test_garbage_advert_id_ignored(self, _):
        self._post(advert_id='garbage')
        self.assertIsNone(Message.objects.get().advert_id)


@override_settings(CACHES=_DUMMY_CACHE)
class MessagesInboxThreadTests(TestCase):
    def setUp(self):
        self.user = _make_user('inboxuser')
        self.other = _make_user('inboxother')
        now = timezone.now()
        self.msg_in = Message.objects.create(
            sender_id=self.other.pk, recipient_id=self.user.pk,
            advert_id=None, text='Привет', is_read=False, created_at=now,
        )
        self.msg_out = Message.objects.create(
            sender_id=self.user.pk, recipient_id=self.other.pk,
            advert_id=None, text='И вам привет', is_read=False,
            created_at=now + timezone.timedelta(minutes=1),
        )
        self.client_ = _client_for(self.user)

    def test_inbox_groups_conversations(self):
        resp = self.client_.get('/messages/')
        self.assertEqual(resp.status_code, 200)
        conversations = resp.context['conversations']
        self.assertEqual(len(conversations), 1)
        conv = conversations[0]
        self.assertEqual(conv['other_user'].pk, self.other.pk)
        self.assertEqual(conv['unread_count'], 1)

    def test_thread_marks_incoming_as_read(self):
        resp = self.client_.get(f'/messages/{self.other.pk}/')
        self.assertEqual(resp.status_code, 200)
        self.msg_in.refresh_from_db()
        self.msg_out.refresh_from_db()
        self.assertTrue(self.msg_in.is_read)
        self.assertFalse(self.msg_out.is_read)  # своё исходящее не трогаем

    def test_unread_count_api(self):
        resp = self.client_.get('/api/messages/unread/')
        self.assertEqual(resp.json(), {'ok': True, 'count': 1})
        anon = Client().get('/api/messages/unread/')
        self.assertEqual(anon.json(), {'ok': False, 'count': 0})
