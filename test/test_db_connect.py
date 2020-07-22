import unittest
from db_connect import insert_into_hashtags
import mock

class TestDbConnect(unittest.TestCase):

    @mock.patch('db_connect.MySQLdb')
    def test_insert_into_hashtags_connection_fail(self, mock_db):
        mock_db.connect.side_effect = Exception
        data = 'dummy'
        with self.assertRaises(Exception):
            insert_into_hashtags()

    @mock.patch('db_connect.MySQLdb')
    def test_insert_into_hashtags_success(self, mock_db):
        data = 'dummy'
        insert_into_hashtags(data)
        mock_db.connect.assert_called_once()
        mock_db.connect().commit.assert_called_once()
        mock_db.connect().cursor.assert_called_once()
        mock_db.connect().cursor().executemany.assert_called_once()
        mock_db.connect().cursor().close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
