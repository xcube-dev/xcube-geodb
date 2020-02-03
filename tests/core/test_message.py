import unittest

from xcube_geodb.core.message import Message


class TestMessage(unittest.TestCase):
    def test_message(self):
        message = Message('helge ist doof')
        self.assertEqual('helge ist doof', message.message)


if __name__ == '__main__':
    unittest.main()
