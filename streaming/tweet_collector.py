#!/usr/bin/env python

from __future__ import print_function

# import sys
# import nltk
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def get_tweet_row(tweet, type="tweet", v_or_e=None):
    """
    Transform a tweet JSON object into a list of tweet features, in specified format:
    Formats can be either:
     - a tweet row
     - a user graph where edges are RT, replies or mentions between two users
     - a hashtag graph where edges occur between hastags if they are in the same tweet.
    Parameters
    ----------
    tweet : tweet JSON object
    type : tweet row, user edge, user vertex, hastag edge or hastag vertex

    Returns
    -------
    if 'tweet':
        list: user ID, username, user location, timestamp, tweet ID, boolean for retweet, tweet text,
              tweet language, list of other users mentioned by ID, list of other users mentioned by username,
              list of hashtags, list of urls links.
    if 'tweeter_vertices':
        list of all mentioned users, by ID and username
    if 'tweeter_edges':
        list of pairs of users that represent edges, by ID and username
    """

    # user
    user = tweet['user']['screen_name']
    user_id = tweet['user']['id_str']
    location = tweet['user']['location']
    # tweet
    timestamp = tweet['timestamp_ms']
    user_mentions = [user['screen_name'] for user in tweet['entities']['user_mentions']] if tweet[
                                                                                                'truncated'] is False else [
        user['screen_name'] for user in tweet['extended_tweet']['entities']['user_mentions']]
    user_mention_ids = [user['id_str'] for user in tweet['entities']['user_mentions']] if tweet[
                                                                                              'truncated'] is False else [
        user['id_str'] for user in tweet['extended_tweet']['entities']['user_mentions']]
    language = tweet['lang']

    try:
        tweet['retweeted_status']
        retweet = True
    except:
        retweet = False

    tweet_id = tweet['id_str'] if retweet == False else tweet['retweeted_status']['id_str']

    if retweet is False:
        if tweet['truncated'] is False:
            text = tweet['text']
            hashtags = None if tweet['entities']['hashtags'] == [] else [hashtag["text"] for hashtag in
                                                                         tweet["entities"]["hashtags"]]
            urls = None if tweet['entities']['urls'] == [] else [url['expanded_url'] for url in
                                                                 tweet['entities']['urls']]

        else:
            text = tweet['extended_tweet']['full_text']
            hashtags = None if tweet['extended_tweet']['entities']['hashtags'] == [] else [hashtag["text"] for
                                                                                           hashtag in
                                                                                           tweet["extended_tweet"][
                                                                                               "entities"][
                                                                                               "hashtags"]]
            urls = None if tweet['extended_tweet']['entities']['urls'] == [] else [url['expanded_url'] for url in
                                                                                   tweet['extended_tweet'][
                                                                                       'entities']['urls']]


    else:
        tweet_rt = tweet['retweeted_status']
        if tweet_rt['truncated'] is False:
            text = tweet_rt['text']
            hashtags = None if tweet_rt['entities']['hashtags'] == [] else [hashtag["text"] for hashtag in
                                                                            tweet_rt["entities"]["hashtags"]]
            urls = None if tweet_rt['entities']['urls'] == [] else [url['expanded_url'] for url in
                                                                    tweet_rt['entities']['urls']]

        else:
            text = tweet_rt['extended_tweet']['full_text']
            hashtags = None if tweet_rt['extended_tweet']['entities']['hashtags'] == [] else [hashtag["text"] for
                                                                                              hashtag in tweet_rt[
                                                                                                  "extended_tweet"][
                                                                                                  "entities"][
                                                                                                  "hashtags"]]
            urls = None if tweet_rt['extended_tweet']['entities']['urls'] == [] else [url['expanded_url'] for url in
                                                                                      tweet_rt['extended_tweet'][
                                                                                          'entities']['urls']]

    if type == "tweet":
        return [user, location, timestamp, tweet_id, retweet, text, language, user_mentions, hashtags,
                urls]

    elif type == "tweeter_graph":
        if v_or_e == 'vertices':
            vertex_names = [user] + user_mentions
            # vertex_ids = user_id + user_mention_ids
            return vertex_names  # [[id, name] for id, name in zip(vertex_ids, vertex_names)]
        elif v_or_e == 'edges':
            return [[mentioned, user, timestamp, tweet_id, retweet, text, language, hashtags, urls] for mentioned in
                    user_mentions]

    elif type == "hashtag_graph":
        if v_or_e == 'vertices':
            return hashtags
        elif v_or_e == 'edges':
            return [[h1, h2] for h1 in hastags for h2 in hashtags if h1 < h2]


def apply_function(x, function, type, v_or_e=None):
    try:
        return function(x, type, v_or_e)
    except:
        return ''


def transform_to_tsv(x, type='tweet'):
    if type == 'tweet':
        string_list = [",".join(item) if isinstance(item, list) else str(item) for item in x]

    elif type == 'edges':
        str_edge = [[str(item) for item in user] for user in x]
        string_list = [",".join(item) if isinstance(item, list) else str(item) for item in str_edge]

    elif type == 'vertices':
        string_list = x

    try:
        tsv = "\t\t".join(string_list)
    except:
        tsv = ""
    return tsv


if __name__ == "__main__":

    import argparse

    def str2bool(v):
        if isinstance(v, bool):
            return v
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')


    parser = argparse.ArgumentParser()
    parser.add_argument("--interval_length", type=int, default=10,
                        help="set the length of time intervals, number of seconds")
    parser.add_argument("--num_partitions", type=int, default=2,
                        help="set the number of partitions the RDD has before it is save to textfiles")
    parser.add_argument("--path", help="set the path where the files are saved")
    parser.add_argument("--prefix", default='/file', help="prefix for saved textfiles")
    parser.add_argument("--topic", default='twitter-stream', help="name of kafka topic to connect to")
    parser.add_argument("--graph", type=str2bool, default=False, help="True if return graph data else Tweet data")
    parser.add_argument("--graph_type", help="choose tweeter_graph or hashtag_graph")

    args = parser.parse_args()
    if args.interval_length:
        interval_length = args.interval_length
    num_partitions = args.num_partitions
    if args.path:
        path = args.path
    prefix = args.prefix
    topic = args.topic
    graph = args.graph
    if args.graph_type:
        graph_type = args.graph_type

    file_path = path + prefix

    sc = SparkContext()
    ssc = StreamingContext(sc, interval_length)  # set length of intervals

    zkQuorum = "localhost:2181"
    kafka_stream = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    tweets = kafka_stream.map(lambda x: json.loads(x[1]))
    # count tweets per batch
    tweets.count().pprint()

    #for 'tweet rows'
    if graph is False:
        tweet_rows = tweets.map(lambda x: apply_function(x, get_tweet_row, 'tweet'))
        tweet_rows.pprint(1)

        tweet_csv = tweet_rows.map(lambda x: transform_to_tsv(x, type ='tweet'))
        tweet_csv.pprint(1)
        tweet_csv.repartition(num_partitions).saveAsTextFiles(file_path)  # set number of partitions

    #for user graph or hashtag graph
    else:
        vertices = tweets.map(lambda x: apply_function(x, get_tweet_row, graph_type, 'vertices'))
        vertices.pprint(1)
        edges = tweets.map(lambda x: apply_function(x, get_tweet_row, graph_type, 'edges'))
        edges.pprint(1)

        vertices_tsv = vertices.map(lambda x: transform_to_tsv(x, type ='vertices'))
        vertices_tsv.pprint(1)
        vertices_tsv.repartition(num_partitions).saveAsTextFiles(path + "/vertices" + prefix)

        edges_tsv = edges.map(lambda x: transform_to_tsv(x, type ='edges'))
        edges_tsv.pprint(1)
        edges_tsv.repartition(num_partitions).saveAsTextFiles(path + "/edges" + prefix)  # set number of partitions

    ssc.start()
    ssc.awaitTermination()
