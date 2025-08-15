import unittest

from xcube_geodb.core.message import Message


class TestMessage(unittest.TestCase):
    def test_message(self):
        message = Message("just a test message")
        self.assertDictEqual({"Message": "just a test message"}, message.to_dict())
