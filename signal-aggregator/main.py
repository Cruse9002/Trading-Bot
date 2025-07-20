import pika
import json
import threading
import time
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("SignalAggregator")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
TA_QUEUE = os.environ.get("TA_SIGNAL_QUEUE", "ta_signals")
SENTIMENT_QUEUE = os.environ.get("SENTIMENT_SIGNAL_QUEUE", "sentiment_signals")
AGG_QUEUE = os.environ.get("AGGREGATED_SIGNAL_QUEUE", "aggregated_signals")

latest_signals = {}
lock = threading.Lock()

def get_rabbitmq_channel():
    for attempt in range(5):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=TA_QUEUE, durable=True)
            channel.queue_declare(queue=SENTIMENT_QUEUE, durable=True)
            channel.queue_declare(queue=AGG_QUEUE, durable=True)
            return connection, channel
        except Exception as e:
            logger.error(f"RabbitMQ connection failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    raise Exception("Failed to connect to RabbitMQ after multiple attempts.")

def ta_callback(ch, method, properties, body):
    try:
        signal = json.loads(body)
        asset = signal.get("symbol", "unknown")
        with lock:
            if asset not in latest_signals:
                latest_signals[asset] = {}
            latest_signals[asset]["ta"] = signal
        logger.info(f"Updated TA signal for {asset}: {signal}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"TA callback error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def sentiment_callback(ch, method, properties, body):
    try:
        signal = json.loads(body)
        asset = signal.get("asset", "unknown")
        with lock:
            if asset not in latest_signals:
                latest_signals[asset] = {}
            latest_signals[asset]["sentiment"] = signal
        logger.info(f"Updated Sentiment signal for {asset}: {signal}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Sentiment callback error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def publisher():
    _, channel = get_rabbitmq_channel()
    while True:
        with lock:
            for asset, signals in latest_signals.items():
                if "ta" in signals and "sentiment" in signals:
                    agg = {
                        "asset": asset,
                        "ta": signals["ta"],
                        "sentiment": signals["sentiment"],
                        "timestamp": time.time()
                    }
                    try:
                        channel.basic_publish(
                            exchange='',
                            routing_key=AGG_QUEUE,
                            body=json.dumps(agg),
                            properties=pika.BasicProperties(delivery_mode=2)
                        )
                        logger.info(f"Published aggregated signal: {agg}")
                    except Exception as e:
                        logger.error(f"Error publishing aggregated signal: {e}")
        time.sleep(10)

def main():
    logger.info("Starting Signal Aggregator...")
    connection, channel = get_rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=TA_QUEUE, on_message_callback=ta_callback)
    channel.basic_consume(queue=SENTIMENT_QUEUE, on_message_callback=sentiment_callback)
    pub_thread = threading.Thread(target=publisher, daemon=True)
    pub_thread.start()
    logger.info("Waiting for signals...")
    try:
        channel.start_consuming()
    except Exception as e:
        logger.error(f"Aggregator error: {e}")
        time.sleep(10)
        main()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped.") 