import praw
import pika
import json
import os
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("SocialMediaCollector")

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "crypto-bot/0.1")
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = os.environ.get("SOCIAL_DATA_QUEUE", "raw_social_data")
KEYWORDS = ["BTC", "Bitcoin", "ETH", "Ethereum", "Solana", "crypto", "$BTC", "$ETH"]

# RabbitMQ setup
def get_rabbitmq_channel():
    for attempt in range(5):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            return connection, channel
        except Exception as e:
            logger.error(f"RabbitMQ connection failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    raise Exception("Failed to connect to RabbitMQ after multiple attempts.")

def get_reddit_instance():
    for attempt in range(5):
        try:
            reddit = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent=REDDIT_USER_AGENT
            )
            # Test connection
            _ = reddit.user.me()
            return reddit
        except Exception as e:
            logger.error(f"Reddit API connection failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    raise Exception("Failed to connect to Reddit API after multiple attempts.")

def main():
    logger.info("Connecting to Reddit API and RabbitMQ...")
    reddit = get_reddit_instance()
    connection, channel = get_rabbitmq_channel()
    subreddit = reddit.subreddit("all")
    while True:
        try:
            for submission in subreddit.stream.submissions(skip_existing=True):
                try:
                    text = (submission.title + " " + submission.selftext).lower()
                    if any(keyword.lower() in text for keyword in KEYWORDS):
                        post = {
                            "source": "reddit",
                            "id": submission.id,
                            "title": submission.title,
                            "text": submission.selftext,
                            "created_utc": submission.created_utc,
                            "url": submission.url
                        }
                        channel.basic_publish(
                            exchange='',
                            routing_key=QUEUE_NAME,
                            body=json.dumps(post),
                            properties=pika.BasicProperties(delivery_mode=2)
                        )
                        logger.info(f"Published: {post}")
                except Exception as e:
                    logger.error(f"Error processing submission: {e}")
        except Exception as e:
            logger.error(f"Reddit stream error: {e}. Reconnecting in 10 seconds...")
            time.sleep(10)
            reddit = get_reddit_instance()
            connection, channel = get_rabbitmq_channel()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped.") 