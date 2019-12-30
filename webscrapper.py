from bs4 import BeautifulSoup
import pandas as pd
import requests
import redis


def convert_tweet_to_dict(tweet):
    tweet_object = {
        "author": tweet.find('h2', attrs={"class": "author"}).text,
        "date": tweet.find('h5', attrs={"class": "dateTime"}).text,
        "tweet": tweet.find('p', attrs={"class": "content"}).text,
        "likes": tweet.find('p', attrs={"class": "likes"}).text,
        "shares": tweet.find('p', attrs={"class": "shares"}).text
    }
    return tweet_object


def get_tweets(html_content):
    tweet_data = [i for i in html_content.findAll('div', attrs={"class": "tweetcontainer"})]
    tweets = list(map(convert_tweet_to_dict, tweet_data))

    tweets_pd = pd.DataFrame(tweets)
    tweets_pd.reset_index(inplace=True)

    return tweets_pd


def convert_dict_keys_to_str(tweets_pd):
    tweets_pd['index'] = tweets_pd["index"].apply(lambda x: str(x))


def insert_data(tweet_dict):
    tweet_dict_index = tweet_dict['index']
    del tweet_dict['index']
    redis_connection.hmset(tweet_dict_index, tweet_dict.to_dict())


def query_redis_database(data_key):
    data = redis_connection.hgetall(data_key)
    return data


def main():
    global redis_connection

    # URL To Mock Dataset
    url = 'http://ethans_fake_twitter_site.surge.sh/'

    response = requests.get(url, timeout=5)
    content = BeautifulSoup(response.content, "html.parser")
    redis_connection = redis.Redis(db=4, decode_responses=True)

    tweets_pd = get_tweets(content)
    tweets_pd.apply(lambda x: insert_data(x), axis=1)

    # List of all object keys
    redis_keys = redis_connection.keys()

    redis_data = list(map(query_redis_database, redis_keys))
    print(redis_data)
