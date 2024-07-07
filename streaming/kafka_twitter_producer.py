#!/usr/bin/env python

# import required libraries
from kafka import KafkaProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import time
import traceback

# update the following to your own key and token
consumer_key = "ZFILB24IUKzJUhcu847foBj5b"
consumer_secret = "YItuGgmEzv5YZWJ1RD8dXKzVQIXJkvRv8D3LvpeGwdIeSrJmDT"
access_token = "1244333195476250626-ZTPBFy0Yzb4tbM06wAPnkn6ZgubtmX"
access_token_secret = "dASL1QQJ5ftB8woFZxnq75RdAsro6pK0sncuReN7P2MaL"

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
#This is a basic listener that just sends received tweets to kafka
class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send(topic, data.encode('utf-8'))
        print(len(data))
        return True

    def on_error(self, status):
        print(status)
        return False

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default='twitter-stream', help="name of kafka topic to connect to")
    parser.add_argument("--filters", default= None, nargs='+')

    args = parser.parse_args()
    topic = args.topic
    filter_words = args.filters

    print('Creating Kafka topic: ' + topic)
    if filter_words is None:
        print('Collecting all incoming tweets')
    else:
        print('Collecting tweets that contain the word/s: ' + str(filter_words))
    #This handles Twitter authetification and the connection to Twitter Streaming API

    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    # Goal is to keep this process always going
    while True:
        try:
            if filter_words is None:
                stream.sample()
            else:
                stream.filter(track=filter_words)
        except:
            print(traceback.format_exc())
        time.sleep(10)
