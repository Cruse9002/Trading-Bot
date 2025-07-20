import pika
import json
import os
import time
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("PositionMonitor")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
UPDATE_QUEUE = os.environ.get("POSITION_UPDATE_QUEUE", "position_updates")
POSITIONS_FILE = os.environ.get("POSITIONS_FILE", "open_positions.json")
BINANCE_API_URL = os.environ.get("BINANCE_API_URL", "https://api.binance.com/api/v3/ticker/price?symbol={symbol}")


def get_rabbitmq_channel():
    for attempt in range(5):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=UPDATE_QUEUE, durable=True)
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

def get_current_price(asset):
    symbol = asset.replace("/", "").upper()
    url = BINANCE_API_URL.format(symbol=symbol)
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        return float(resp.json()["price"])
    except Exception as e:
        logger.error(f"Error fetching price for {asset}: {e}")
        return None

def main():
    logger.info("Starting position monitor...")
    connection, channel = get_rabbitmq_channel()
    while True:
        try:
            positions = load_positions()
            updated_positions = []
            for pos in positions:
                asset = pos.get("asset", "BTCUSDT")
                entry = pos.get("entry", 100)
                stop_loss = pos.get("stop_loss", 95)
                take_profit = pos.get("take_profit", 110)
                size = pos.get("position_size", 1)
                current_price = get_current_price(asset)
                if current_price is None:
                    logger.warning(f"Skipping PnL update for {asset} due to missing price.")
                    updated_positions.append(pos)
                    continue
                pnl = (current_price - entry) * size if pos.get("side") == "LONG" else (entry - current_price) * size
                pos_update = pos.copy()
                pos_update["current_price"] = current_price
                pos_update["pnl"] = pnl
                closed = False
                if (pos.get("side") == "LONG" and current_price <= stop_loss) or (pos.get("side") == "SHORT" and current_price >= stop_loss):
                    pos_update["status"] = "CLOSED_SL"
                    closed = True
                elif (pos.get("side") == "LONG" and current_price >= take_profit) or (pos.get("side") == "SHORT" and current_price <= take_profit):
                    pos_update["status"] = "CLOSED_TP"
                    closed = True
                else:
                    pos_update["status"] = "OPEN"
                try:
                    channel.basic_publish(
                        exchange='',
                        routing_key=UPDATE_QUEUE,
                        body=json.dumps(pos_update),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    logger.info(f"Position update: {pos_update}")
                except Exception as e:
                    logger.error(f"Error publishing position update: {e}")
                if not closed:
                    updated_positions.append(pos)
            save_positions(updated_positions)
            time.sleep(30)
        except Exception as e:
            logger.error(f"Position monitor loop error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped.") 