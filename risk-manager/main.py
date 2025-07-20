import pika
import json
import os
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("RiskManager")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
ORDER_QUEUE = os.environ.get("RAW_ORDER_QUEUE", "raw_orders")
RISK_QUEUE = os.environ.get("RISK_CHECKED_ORDER_QUEUE", "risk_checked_orders")
CAPITAL = float(os.environ.get("CAPITAL", 10000))
RISK_PER_TRADE = float(os.environ.get("RISK_PER_TRADE", 0.01))


def get_rabbitmq_channel():
    for attempt in range(5):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=ORDER_QUEUE, durable=True)
            channel.queue_declare(queue=RISK_QUEUE, durable=True)
            return connection, channel
        except Exception as e:
            logger.error(f"RabbitMQ connection failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    raise Exception("Failed to connect to RabbitMQ after multiple attempts.")

def main():
    logger.info("Starting Risk Manager...")
    connection, channel = get_rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        try:
            order = json.loads(body)
            risk_amount = CAPITAL * RISK_PER_TRADE
            entry = 100
            stop_loss = 95
            take_profit = 110
            position_size = risk_amount / abs(entry - stop_loss)
            order.update({
                "position_size": position_size,
                "entry": entry,
                "stop_loss": stop_loss,
                "take_profit": take_profit
            })
            channel.basic_publish(
                exchange='',
                routing_key=RISK_QUEUE,
                body=json.dumps(order),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.info(f"Published risk-checked order: {order}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Risk manager callback error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=ORDER_QUEUE, on_message_callback=callback)
    logger.info("Waiting for orders...")
    try:
        channel.start_consuming()
    except Exception as e:
        logger.error(f"Risk manager error: {e}")
        time.sleep(10)
        main()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped.") 