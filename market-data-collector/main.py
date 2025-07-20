import asyncio
import websockets
import pika
import json
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("MarketDataCollector")

BINANCE_WS_URL = os.environ.get("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@trade")
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = os.environ.get("MARKET_DATA_QUEUE", "raw_market_data")

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
            asyncio.sleep(2 ** attempt)
    raise Exception("Failed to connect to RabbitMQ after multiple attempts.")

async def main():
    logger.info("Connecting to Binance websocket...")
    connection, channel = get_rabbitmq_channel()
    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL) as ws:
                async for message in ws:
                    try:
                        data = json.loads(message)
                        tick = {
                            "exchange": "binance",
                            "symbol": "BTCUSDT",
                            "price": float(data["p"]),
                            "quantity": float(data["q"]),
                            "timestamp": int(data["T"])
                        }
                        channel.basic_publish(
                            exchange='',
                            routing_key=QUEUE_NAME,
                            body=json.dumps(tick),
                            properties=pika.BasicProperties(delivery_mode=2)
                        )
                        logger.info(f"Published: {tick}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
        except Exception as e:
            logger.error(f"Websocket connection error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped.") 