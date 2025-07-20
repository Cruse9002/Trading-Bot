import pika
import json
import os
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ExecutionHandler")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
ORDER_QUEUE = os.environ.get("RISK_CHECKED_ORDER_QUEUE", "risk_checked_orders")
EXEC_REPORT_QUEUE = os.environ.get("EXECUTED_ORDER_QUEUE", "executed_orders")
POSITIONS_FILE = os.environ.get("POSITIONS_FILE", "open_positions.json")


def get_rabbitmq_channel():
    for attempt in range(5):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=ORDER_QUEUE, durable=True)
            channel.queue_declare(queue=EXEC_REPORT_QUEUE, durable=True)
            return connection, channel
        except Exception as e:
            logger.error(f"RabbitMQ connection failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    raise Exception("Failed to connect to RabbitMQ after multiple attempts.")

def load_positions():
    try:
        with open(POSITIONS_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load positions file: {e}")
        return []

def save_positions(positions):
    try:
        with open(POSITIONS_FILE, "w") as f:
            json.dump(positions, f)
    except Exception as e:
        logger.error(f"Could not save positions file: {e}")

def main():
    logger.info("Starting paper trading handler...")
    connection, channel = get_rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        try:
            order = json.loads(body)
            order["status"] = "FILLED"
            order["fill_time"] = time.time()
            positions = load_positions()
            positions.append(order)
            save_positions(positions)
            channel.basic_publish(
                exchange='',
                routing_key=EXEC_REPORT_QUEUE,
                body=json.dumps(order),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.info(f"Executed order: {order}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Execution handler callback error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=ORDER_QUEUE, on_message_callback=callback)
    logger.info("Waiting for risk-checked orders...")
    try:
        channel.start_consuming()
    except Exception as e:
        logger.error(f"Execution handler error: {e}")
        time.sleep(10)
        main()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped.") 