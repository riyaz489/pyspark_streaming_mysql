import unittest
from twitter_processor import *
import mock
import twitter_processor


class MockStreamContext:

    def stop(self, *args, **kwargs):
        pass


class MockRdd:

    def filter(self, *args, **kwargs):
        return MockRdd()

    def collect(self):
        return [{'hash_tags': [{'text': ''}], 'place': 'sdf'},
                {'hash_tags': [{'text': '❤♫'}], 'place': 'sdf'},
                {'hash_tags': [{'text': 'd'}, {'text': 'er'}, {'text': 'df'}],
                 'created_at': '2020-03-20',
                 'tweet_id': '134',
                 'place_id': 'place_id',
                 'place': 'sdf'}
                ]


class TestTwitterProcessor(unittest.TestCase):

    def setUp(self):
        self.cl = CustomListener()
        # setting global variable ssc value
        twitter_processor.ssc = MockStreamContext()

    @mock.patch('builtins.print')
    def test_on_receive_error(self, mock_print):
        self.cl.onReceiverError('dummy')
        mock_print.assert_called_once()

    @mock.patch('builtins.print')
    def test_on_receive_stopped(self, mock_print):
        self.cl.onReceiverStopped('dummy')
        mock_print.assert_called_once()

    def test_is_english_true(self):
        self.assertEqual(is_english('HI'), True)

    def test_is_english_false(self):
        self.assertEqual(is_english('❤♫'), False)

    @mock.patch('twitter_processor.insert_into_hashtags')
    @mock.patch('builtins.print')
    def test_process_rdd(self, mock_print, mock_insert_data):
        process_rdd('dummy', MockRdd())
        self.assertEqual(mock_print.call_count, 3)
        mock_insert_data.assert_called_once()


if __name__ == '__main__':
    unittest.main()
