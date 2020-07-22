import unittest
from main import *
import mock


class MockTCPConnection:
    def send(self, *args, **kwargs):
        pass

class MockResponse:
    def iter_lines(self, *args, **kwargs):
        return [1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7]


class TestMain(unittest.TestCase):

    @mock.patch('builtins.print')
    @mock.patch('main.requests')
    def test_get_tweets(self, mock_req, mock_print):
        mock_req.get.return_value = 'dummy'
        auth = ''
        res = get_tweets(auth=auth)
        self.assertEqual(res, 'dummy')
        mock_print.assert_called_once()
        mock_req.get.assert_called_once()

    @mock.patch('builtins.print')
    @mock.patch('main.requests')
    def test_get_tweets_req_fails(self, mock_req, mock_print):
        mock_req.get.side_effect = Exception
        auth = ''
        with self.assertRaises(Exception):
            get_tweets(auth=auth)

    @mock.patch('main.json')
    @mock.patch('builtins.print')
    def test_send_tweets_to_spark(self, mock_print, mock_json):
        http_resp = MockResponse()
        tcp_connection = MockTCPConnection()
        t_in_sec = 1
        mock_json.dumps.return_value = 'dummy'
        mock_json.loads.return_value = {
                'created_at': 'created_at',
                'id_str': 'id_str',
                'place': {'name': 'place', 'id': 'id'},
                'entities': {'hashtags': ['12', 'asd']}
        }
        send_tweets_to_spark(http_resp=http_resp, tcp_connection=tcp_connection, t_in_sec=t_in_sec)
        self.assertEqual(mock_print.call_count, 42)
        self.assertEqual(mock_json.loads.call_count, 21)

    @mock.patch('main.time')
    def test_send_tweets_to_spark_time_ends(self, mock_time):
        http_resp = MockResponse()
        tcp_connection = MockTCPConnection()
        t_in_sec = 1
        mock_time.time.side_effect = [1, 2]
        send_tweets_to_spark(http_resp=http_resp, tcp_connection=tcp_connection, t_in_sec=t_in_sec)
        self.assertEqual(mock_time.time.call_count, 2)

    @mock.patch('main.json')
    @mock.patch('builtins.print')
    def test_send_tweets_to_spark_exception(self, mock_print, mock_json):
        http_resp = MockResponse()
        tcp_connection = MockTCPConnection()
        t_in_sec = 1
        mock_json.loads.side_effect = Exception
        send_tweets_to_spark(http_resp=http_resp, tcp_connection=tcp_connection, t_in_sec=t_in_sec)
        self.assertEqual(mock_print.call_count, 21)

    @mock.patch('builtins.print')
    @mock.patch('main.socket')
    def test_create_socket(self, mock_socket, mock_print):
        mock_socket.socket.return_value.accept.return_value = 'dummy', 'address'
        result = create_socket()
        self.assertEqual(result, 'dummy')
        mock_socket.socket.assert_called_once()
        mock_socket.socket().bind.assert_called_once()
        mock_socket.socket().listen.assert_called_once()
        mock_socket.socket().accept.assert_called_once()
        self.assertEqual(mock_print.call_count, 3)


if __name__ == '__main__':
    unittest.main()
