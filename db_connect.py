import MySQLdb


def insert_into_hashtags(data):
    """
    this method is used to store data into hashtags table.
    :param data: list, list of hashtags dictionaries.
    """
    db_conn = MySQLdb.connect("localhost", "root", "", "twitter")
    cursor = db_conn.cursor()
    cursor.executemany("""INSERT INTO hashtags(hashtag, created_at, tweet_id, place_id, place)VALUES
    (%(hash_tags)s, %(created_at)s, %(tweet_id)s, %(place_id)s, %(place)s)""", data)
    # close the connection to the database.
    db_conn.commit()
    cursor.close()
