import os
import pika
import json
from pymongo import MongoClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, timedelta
import logging
import time
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("NLPSentimentModule")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = os.environ.get("SENTIMENT_SIGNAL_QUEUE", "sentiment_signals")
MONGODB_HOST = os.environ.get("MONGODB_HOST", "mongodb")
MONGODB_PORT = int(os.environ.get("MONGODB_PORT", 27017))
MONGODB_DB = os.environ.get("MONGODB_DB", "raw_data_lake")
USE_DL_SENTIMENT = os.environ.get("USE_DL_SENTIMENT", "false").lower() == "true"

# Placeholder for DL model integration
def load_dl_sentiment_model():
    # from dl_models.sentiment_model import predict_sentiment
    logger.info("[DL] Loading DL-based Sentiment model (stub)")
    return lambda text: {"compound": float(np.random.uniform(-1, 1))}

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

def fetch_recent_posts(collection_name, minutes=10):
    for attempt in range(5):
        try:
            client = MongoClient(MONGODB_HOST, MONGODB_PORT, serverSelectionTimeoutMS=5000)
            db = client[MONGODB_DB]
            collection = db[collection_name]
            since = datetime.utcnow() - timedelta(minutes=minutes)
            return list(collection.find({"created_utc": {"$gte": since.timestamp()}}))
        except Exception as e:
            logger.error(f"MongoDB fetch error (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    return []

def main():
    logger.info("Starting sentiment analysis loop...")
    analyzer = SentimentIntensityAnalyzer()
    dl_sentiment_predict = load_dl_sentiment_model() if USE_DL_SENTIMENT else None
    connection, channel = get_rabbitmq_channel()
    while True:
        try:
            posts = fetch_recent_posts("social_posts") + fetch_recent_posts("news_articles")
            for post in posts:
                text = post.get("text") or post.get("description") or ""
                if not text:
                    continue
                if USE_DL_SENTIMENT and dl_sentiment_predict:
                    sentiment = dl_sentiment_predict(text)
                else:
                    sentiment = analyzer.polarity_scores(text)
                signal = {
                    "asset": post.get("title", "unknown"),
                    "sentiment_score": sentiment["compound"],
                    "source": post.get("source", "unknown"),
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }
                channel.basic_publish(
                    exchange='',
                    routing_key=QUEUE_NAME,
                    body=json.dumps(signal),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                logger.info(f"Published sentiment signal: {signal}")
            time.sleep(300)
        except Exception as e:
            logger.error(f"Sentiment loop error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped.") 