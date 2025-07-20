import pika
import json
import os
import logging
from pymongo import MongoClient
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("TextDataConsumer")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
SOCIAL_QUEUE = os.environ.get("SOCIAL_DATA_QUEUE", "raw_social_data")
NEWS_QUEUE = os.environ.get("NEWS_DATA_QUEUE", "raw_news_data")
MONGODB_HOST = os.environ.get("MONGODB_HOST", "mongodb")
MONGODB_PORT = int(os.environ.get("MONGODB_PORT", 27017))
MONGODB_DB = os.environ.get("MONGODB_DB", "raw_data_lake")

# MongoDB setup
def get_mongo_collection(collection_name):
    for attempt in range(5):
        try:
            client = MongoClient(MONGODB_HOST, MONGODB_PORT, serverSelectionTimeoutMS=5000)
            db = client[MONGODB_DB]
            # Test connection
            db.command("ping")
            return db[collection_name]
        except Exception as e:
            logger.error(f"MongoDB connection failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    raise Exception("Failed to connect to MongoDB after multiple attempts.")

def get_rabbitmq_channel():
    for attempt in range(5):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=SOCIAL_QUEUE, durable=True)
            channel.queue_declare(queue=NEWS_QUEUE, durable=True)
            return connection, channel
        except Exception as e:
            logger.error(f"RabbitMQ connection failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    raise Exception("Failed to connect to RabbitMQ after multiple attempts.")

def make_callback(collection):
    def callback(ch, method, properties, body):
        try:
            doc = json.loads(body)
            collection.insert_one(doc)
            logger.info(f"Inserted into {collection.name}: {doc}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    return callback

def main():
    logger.info("Connecting to RabbitMQ and MongoDB...")
    social_collection = get_mongo_collection("social_posts")
    news_collection = get_mongo_collection("news_articles")
    connection, channel = get_rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=SOCIAL_QUEUE, on_message_callback=make_callback(social_collection))
    channel.basic_consume(queue=NEWS_QUEUE, on_message_callback=make_callback(news_collection))
    logger.info("Waiting for messages...")
    try:
        channel.start_consuming()
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        time.sleep(10)
        main()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped.") 