import pika
import json
import os
import logging
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("MarketDataConsumer")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = os.environ.get("MARKET_DATA_QUEUE", "raw_market_data")
INFLUXDB_URL = os.environ.get("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN", "my-token")
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG", "my-org")
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET", "market_data")

# InfluxDB setup
def get_influxdb_write_api():
    for attempt in range(5):
        try:
            client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
            return client.write_api(write_options=SYNCHRONOUS)
        except Exception as e:
            logger.error(f"InfluxDB connection failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    raise Exception("Failed to connect to InfluxDB after multiple attempts.")

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

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        point = Point("price_tick") \
            .tag("exchange", data["exchange"]) \
            .tag("symbol", data["symbol"]) \
            .field("price", data["price"]) \
            .field("quantity", data["quantity"]) \
            .time(data["timestamp"], write_precision="ms")
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        logger.info(f"Wrote to InfluxDB: {data}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    logger.info("Connecting to RabbitMQ and InfluxDB...")
    global write_api
    write_api = get_influxdb_write_api()
    connection, channel = get_rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
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