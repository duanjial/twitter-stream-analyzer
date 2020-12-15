import os
import tweepy
import json
import base64
from google.cloud import pubsub_v1

BATCH_SIZE = 2
PROJECT_ID = "tweeter-stream-analyzer"
TOPIC_PATH = f"projects/{PROJECT_ID}/topics/twitter-realtime-stream"
consumer_key = os.getenv("API_KEY")
consumer_secret = os.getenv("API_SECRECT")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRECT")

publisher = pubsub_v1.PublisherClient()


def publish(client, data_lines):
    messages = []
    for line in data_lines:
        messages.append({'data': line})
    body = {'messages': messages}
    str_body = json.dumps(body)
    data = base64.urlsafe_b64encode(bytearray(str_body, 'utf8'))
    future = client.publish(TOPIC_PATH, data=data)
    message_id = future.result()
    print(message_id)


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self, api=None):
        super().__init__()
        self.num_tweets = 0
        self.tweets = []

    def on_status(self, status):
        if hasattr(status, "retweeted_status"):  # Check if Retweet
            return
        else:
            self.num_tweets += 1
            print(f"{status.user.screen_name} tweeted at {status.created_at}")
            try:
                msg = status.extended_tweet["full_text"]
                print(msg)
            except AttributeError:
                msg = status.text
                print(msg)
            # tweet = dict(screen_name=status.user.screen_name,
            #              created_at=status.created_at, msg=msg)
            self.tweets.append(msg)
            if self.num_tweets == BATCH_SIZE:
                # pubsub to publish
                publish(publisher, self.tweets)
                return False

    def on_error(self, status_code):
        if status_code == 420:
            return False


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

myTwitterStreamListener = TwitterStreamListener()
stream = tweepy.Stream(
    auth=api.auth, listener=myTwitterStreamListener, tweet_mode='extended')

stream.filter(track=["manutd", "ole", "manchester united"], is_async=True)
