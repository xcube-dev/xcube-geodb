import unittest

from xcube_geodb.core.message import Message


class TestMessage(unittest.TestCase):
    def test_message(self):
        message = Message('helge ist durchaus klug')
        self.assertEqual('helge ist durchaus klug', message.message)


if __name__ == '__main__':
    unittest.main()
