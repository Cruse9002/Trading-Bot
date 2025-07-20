import requests
import pika
import json
import os
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("NewsFeedCollector")

NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY")
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = os.environ.get("NEWS_DATA_QUEUE", "raw_news_data")
NEWSAPI_URL = os.environ.get("NEWSAPI_URL", "https://newsapi.org/v2/everything")
QUERY = os.environ.get("NEWS_QUERY", "cryptocurrency OR bitcoin OR ethereum OR solana")

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

def fetch_news():
    params = {
        'q': QUERY,
        'apiKey': NEWSAPI_KEY,
        'language': 'en',
        'sortBy': 'publishedAt',
        'pageSize': 10
    }
    try:
        response = requests.get(NEWSAPI_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get('articles', [])
    except Exception as e:
        logger.error(f"Error fetching news: {e}")
        return []

def main():
    logger.info("Starting news polling...")
    connection, channel = get_rabbitmq_channel()
    while True:
        try:
            articles = fetch_news()
            for article in articles:
                try:
                    news = {
                        "source": article.get("source", {}).get("name"),
                        "title": article.get("title"),
                        "description": article.get("description"),
                        "url": article.get("url"),
                        "publishedAt": article.get("publishedAt")
                    }
                    channel.basic_publish(
                        exchange='',
                        routing_key=QUEUE_NAME,
                        body=json.dumps(news),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    logger.info(f"Published: {news}")
                except Exception as e:
                    logger.error(f"Error processing article: {e}")
            logger.info("Sleeping for 5 minutes...")
            time.sleep(300)
        except Exception as e:
            logger.error(f"News polling error: {e}. Reconnecting in 10 seconds...")
            time.sleep(10)
            connection, channel = get_rabbitmq_channel()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped.") 