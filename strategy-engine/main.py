import pika
import json
import os
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("StrategyEngine")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
AGG_QUEUE = os.environ.get("AGGREGATED_SIGNAL_QUEUE", "aggregated_signals")
ORDER_QUEUE = os.environ.get("RAW_ORDER_QUEUE", "raw_orders")
USE_DL_STRATEGY = os.environ.get("USE_DL_STRATEGY", "false").lower() == "true"

# Placeholder for DL strategy integration
def load_dl_strategy():
    logger.info("[DL] Loading DL-based strategy model (stub)")
    return lambda agg: (agg.get("asset", "unknown"), "LONG", "DL_STRATEGY")

def get_rabbitmq_channel():
    for attempt in range(5):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=AGG_QUEUE, durable=True)
            channel.queue_declare(queue=ORDER_QUEUE, durable=True)
            return connection, channel
        except Exception as e:
            logger.error(f"RabbitMQ connection failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    raise Exception("Failed to connect to RabbitMQ after multiple attempts.")

def main():
    logger.info("Starting Strategy Engine...")
    connection, channel = get_rabbitmq_channel()
    dl_strategy = load_dl_strategy() if USE_DL_STRATEGY else None
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        try:
            agg = json.loads(body)
            ta = agg.get("ta", {})
            sentiment = agg.get("sentiment", {})
            asset = agg.get("asset", "unknown")
            rsi = ta.get("value", 50)
            sentiment_score = sentiment.get("sentiment_score", 0)
            if USE_DL_STRATEGY and dl_strategy:
                asset, side, reason = dl_strategy(agg)
            else:
                # Simple rule: LONG if RSI < 30 and sentiment > 0.6
                if rsi < 30 and sentiment_score > 0.6:
                    side = "LONG"
                    reason = f"RSI={rsi}, sentiment={sentiment_score}"
                else:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
            order = {
                "asset": asset,
                "side": side,
                "reason": reason,
                "timestamp": agg.get("timestamp")
            }
            channel.basic_publish(
                exchange='',
                routing_key=ORDER_QUEUE,
                body=json.dumps(order),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.info(f"Published order: {order}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Strategy callback error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=AGG_QUEUE, on_message_callback=callback)
    logger.info("Waiting for aggregated signals...")
    try:
        channel.start_consuming()
    except Exception as e:
        logger.error(f"Strategy engine error: {e}")
        time.sleep(10)
        main()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped.") 