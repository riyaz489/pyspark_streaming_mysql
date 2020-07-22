from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext, StreamingListener
from db_connect import insert_into_hashtags
from constants import SparkStream
import json
from dateutil import parser as date_parser


class CustomListener(StreamingListener):
    """
    this class is used as pySpark listener for data coming from twitter stream.
    """
    def onReceiverError(self, receiver_error):
        """
        this method is used to stop spark stream server on any error.
        :param receiver_error: error attribute.
        """
        print("stream closed")
        ssc.stop(True, False)

    def onReceiverStopped(self, receiver_stopped):
        """
        this method is used to stop spark stream on receiever stop command.
        :param receiver_stopped: receiver attribute.
        """
        print("closing spark server")
        ssc.stop(True, False)


def is_english(data):
    """this class is used to check if current string has only english language characters.
     :param data: string, input string. """
    try:
        data.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True


def process_rdd(time, rdd):
    """
    this method is used to filter twitter data and store it into database.
    :param time: time, time of current batch.
    :param rdd: RDD, current batch rdd.
    """
    dict_data = rdd.filter(lambda x: x['hash_tags'] != [])
    temp = dict_data.collect()
    res_list = []
    for tweet in temp:
        for hashtag in tweet['hash_tags']:
            if len(hashtag['text']) == 0 or len(hashtag['text']) > 200 or len( tweet['place']) > 200:
                print('skipping tweet')
                continue
            # removing other languages and symbols data
            if not is_english(hashtag['text']) or not is_english(tweet['place']):
                print('skipping other language tweets')
                continue
            temp = {
                'created_at': date_parser.parse(tweet['created_at']).strftime('%Y-%m-%d %H:%M:%S'),
                'tweet_id': tweet['tweet_id'],
                'place_id': tweet['place_id'],
                'place': tweet['place'],
                'hash_tags': hashtag['text']
            }
            res_list.append(temp)
    print(res_list)
    if res_list:
        insert_into_hashtags(res_list)


if __name__ == "__main__":

    # creating spark configuration
    conf = SparkConf()
    conf.setAppName(SparkStream.APP_NAME.value)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, int(SparkStream.STREAM_INTERVAL.value))
    ssc.checkpoint(SparkStream.CHECKPOINT.value)

    dataStream = ssc.socketTextStream(SparkStream.TCP_IP.value, SparkStream.TCP_PORT.value)
    cv = dataStream.map(lambda x: json.loads(x))
    cv.foreachRDD(process_rdd)

    # add listener to check if stream closed
    ssc.addStreamingListener(CustomListener())
    ssc.start()
    ssc.awaitTermination()
