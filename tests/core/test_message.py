import unittest

from xcube_geodb.core.message import Message


class TestMessage(unittest.TestCase):
    def test_message(self):
        message = Message("BC-Staff ist durchaus klug")
        self.assertDictEqual(
            {"Message": "BC-Staff ist durchaus klug"}, message.to_dict()
        )
