import socket
import time
import requests
import requests_oauthlib
import json
import yaml
from constants import SparkStream


def get_tweets(auth):
    """
    getting tweets from twitter live stream api.
    :param auth, oAuth, twitter oAuth object for making requests
    :return: response, twitter tweets stream response.
    """
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '='+str(t[1]) for t in query_data])
    res = requests.get(query_url, auth=auth, stream=True)
    print(query_url, res)
    return res


def send_tweets_to_spark(http_resp, tcp_connection, t_in_sec):
    """
    sending twitter stream response to pySpark stream.
    :param http_resp:response, stream response.
    :param tcp_connection:connection, tcp connection for spark streaming.
    :param t_in_sec: int, time in seconds
    """
    end_time = time.time()+t_in_sec
    for line in http_resp.iter_lines():
        if time.time() >= end_time:
            break
        try:
            full_tweet = json.loads(line)
            data_dict = {
                'created_at': full_tweet['created_at'],
                'tweet_id': full_tweet['id_str'],
                'place_id': full_tweet['place']['id'],
                'place': full_tweet['place']['name'],
                'hash_tags': full_tweet['entities']['hashtags']
            }

            print("---------------get tweet--------------------")
            print(full_tweet)
            tcp_connection.send(bytes((json.dumps(data_dict)+"\n"), 'utf-8'))
        except Exception as e:
            print("Error", e)


def create_socket():
    """
     configuring a spark stream using TCP socket.
    :return: connection, pySpark stream connection.
    """
    tcp_ip = SparkStream.TCP_IP.value
    tcp_port = SparkStream.TCP_PORT.value
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((tcp_ip, tcp_port))
    s.listen(1)
    print("Waiting for tcp connection... ")
    conn, address = s.accept()
    print("current address is", address)
    print("Connected ... Starting getting tweets.")
    return conn


if __name__ == '__main__':

    ACCESS_TOKEN = ""
    ACCESS_SECRET = ""
    CONSUMER_KEY = ""
    CONSUMER_SECRET = ""
    with open("./config.yaml", 'r') as yaml_file:
        """
        getting secrets from yaml config file.
        """
        cfg = yaml.load(yaml_file)
        ACCESS_TOKEN = cfg['twitter']['ACCESS_TOKEN']
        ACCESS_SECRET = cfg['twitter']['ACCESS_SECRET']
        CONSUMER_KEY = cfg['twitter']['CONSUMER_KEY']
        CONSUMER_SECRET = cfg['twitter']['CONSUMER_SECRET']
    t = int(input('\n\n\n\n\nenter time in seconds: '))
    connection = create_socket()
    # creating auth for twitter api
    my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
    # getting tweets from twitter
    response = get_tweets(my_auth)
    # sending twitter response to pySpark streaming server
    send_tweets_to_spark(response, connection, t)

