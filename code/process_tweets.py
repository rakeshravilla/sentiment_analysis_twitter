import re
import tweepy
import pymongo
from pymongo import MongoClient, InsertOne, DeleteOne, ReplaceOne, UpdateOne, write_concern
from datetime import datetime
import time

consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

bearer_token = ''


auth = tweepy.OAuth1UserHandler(
    consumer_key, consumer_secret, access_token, access_token_secret
)

api = tweepy.API(auth)

stream = tweepy.Stream(
    consumer_key, consumer_secret, access_token, access_token_secret
)

streaming_client = tweepy.StreamingClient(bearer_token)


public_tweets = api.search_tweets(q=['#layoffs', '#layoff'])


mongo_pwd = ''
client = pymongo.MongoClient("mongodb+srv://rw_user:"+mongo_pwd +
                             "@clustertest.6jkle.mongodb.net/tweets?retryWrites=true&w=majority")
db = client.tweets


def fetch():
    public_tweets = api.search_tweets(q=['#layoffs', '#layoff'])
    return public_tweets


def insert(public_tweets):
    value_dict = {}
    bulk = []
    with client.start_session() as s1:
        with s1.start_transaction():
            tbl_tweet = client.get_database(
                "tweet",
                write_concern=write_concern.WriteConcern(
                    "majority", wtimeout=1000),
            ).tbl_tweet
            for tweets in public_tweets:
                value_dict['_id'] = str(datetime.now().timestamp())
                value_dict['tweet_id'] = tweets.id
                value_dict['tweet_text'] = tweets.text
                for i in tweets.entities['user_mentions']:
                    value_dict['user_screen_name'] = i['screen_name']
                    value_dict['user_id'] = i['id']
                bulk.append(value_dict)
                db.tbl_tweet.insert_one(value_dict, session=s1)


while True:
    public_tweets = fetch()
    insert(public_tweets)

streaming_client.add_rules(tweepy.StreamRule("layoffs"))
streaming_client.filter(threaded=True)

streaming_client.disconnect()

db.tbl_tweet.find_one()

db.tbl_tweet.create_index([('_id', pymongo.ASCENDING)], unique=True)
