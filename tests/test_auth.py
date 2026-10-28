import base64
import json
import unittest
from unittest.mock import patch

from app import auth, config


class AuthenticationTests(unittest.TestCase):
    def setUp(self):
        self.secret = patch.object(config, 'AUTH_SECRET', 'a-test-secret-with-at-least-32-characters')
        self.secret.start()
        self.addCleanup(self.secret.stop)

    def test_round_trip_customer_identity(self):
        self.assertEqual(auth.verify(auth.issue('customer-42')), 'customer-42')

    def test_customer_identity_cannot_be_changed(self):
        token = auth.issue('customer-42')
        payload, signature = token.split('.')
        data = json.loads(base64.urlsafe_b64decode(payload + '=' * (-len(payload) % 4)))
        data['sub'] = 'customer-43'
        forged = base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip('=') + '.' + signature
        with self.assertRaises(ValueError):
            auth.verify(forged)

    def test_expired_token_is_rejected(self):
        with self.assertRaises(ValueError):
            auth.verify(auth.issue('customer-42', ttl=-1))

    def test_incorrect_secret_is_rejected(self):
        token = auth.issue('customer-42')
        with patch.object(config, 'AUTH_SECRET', 'another-test-secret-with-32-characters'):
            with self.assertRaises(ValueError):
                auth.verify(token)

    def test_malformed_tokens_are_rejected(self):
        for token in ('', 'abc', '.', 'a.b.c', 'garbage.signature'):
            with self.subTest(token=token), self.assertRaises(ValueError):
                auth.verify(token)

    def test_unsafe_signing_configuration_is_rejected(self):
        with patch.object(config, 'AUTH_SECRET', ''):
            with self.assertRaises(ValueError):
                auth.issue('customer-42')


if __name__ == '__main__':
    unittest.main()
