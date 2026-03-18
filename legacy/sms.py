"""
SMSC.ru SMS gateway integration.

API docs: https://smsc.ru/api/http/
"""

import logging

import requests
from django.conf import settings

logger = logging.getLogger(__name__)

SMSC_SEND_URL = 'https://smsc.ru/sys/send.php'


def send_sms(phone: str, message: str) -> bool:
    """
    Send an SMS via SMSC.ru.
    Returns True on success, False on failure.
    """
    login = getattr(settings, 'SMSC_LOGIN', '')
    password = getattr(settings, 'SMSC_PASSWORD', '')

    if not login or not password:
        logger.warning('SMSC credentials not configured, SMS not sent to %s', phone)
        return False

    sender = getattr(settings, 'SMSC_SENDER', '')

    params = {
        'login': login,
        'psw': password,
        'phones': phone,
        'mes': message,
        'fmt': 3,        # JSON response
        'charset': 'utf-8',
    }
    if sender:
        params['sender'] = sender

    try:
        resp = requests.post(SMSC_SEND_URL, data=params, timeout=10)
        data = resp.json()

        if 'error' in data:
            logger.error('SMSC error sending to %s: %s (code %s)',
                         phone, data.get('error'), data.get('error_code'))
            return False

        logger.info('SMS sent to %s, id=%s, cnt=%s',
                    phone, data.get('id'), data.get('cnt'))
        return True

    except Exception:
        logger.exception('Failed to send SMS via SMSC to %s', phone)
        return False


def send_otp(phone: str, code: str) -> bool:
    """Send OTP verification code via SMS."""
    message = f'Код подтверждения edunabazar.ru: {code}'
    return send_sms(phone, message)
