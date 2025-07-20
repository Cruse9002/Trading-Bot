import os
import pika
import json
import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("TAModule")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = os.environ.get("TA_SIGNAL_QUEUE", "ta_signals")
INFLUXDB_URL = os.environ.get("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN", "my-token")
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG", "my-org")
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET", "market_data")
SYMBOL = os.environ.get("TA_SYMBOL", "BTCUSDT")
USE_DL_TA = os.environ.get("USE_DL_TA", "false").lower() == "true"

# Placeholder for DL model integration
def load_dl_ta_model():
    # from dl_models.ta_model import predict_ta_signal
    logger.info("[DL] Loading DL-based TA model (stub)")
    return lambda prices: {"indicator": "DL_TA", "value": float(np.random.rand()*100)}

# Classical RSI calculation
def compute_rsi(prices, period=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def fetch_prices():
    for attempt in range(5):
        try:
            client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
            query = f'''from(bucket: "{INFLUXDB_BUCKET}")\n  |> range(start: -1h)\n  |> filter(fn: (r) => r["_measurement"] == "price_tick" and r["symbol"] == "{SYMBOL}")\n  |> filter(fn: (r) => r["_field"] == "price")\n  |> sort(columns: ["_time"])\n'''
            tables = client.query_api().query(query)
            prices = [record.get_value() for table in tables for record in table.records]
            times = [record.get_time() for table in tables for record in table.records]
            df = pd.DataFrame({"price": prices}, index=pd.to_datetime(times))
            return df
        except Exception as e:
            logger.error(f"InfluxDB fetch error (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    return pd.DataFrame()

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

def main():
    logger.info("Starting TA analysis loop...")
    connection, channel = get_rabbitmq_channel()
    dl_ta_predict = load_dl_ta_model() if USE_DL_TA else None
    while True:
        try:
            df = fetch_prices()
            if len(df) < 15:
                logger.warning("Not enough data for TA.")
            else:
                if USE_DL_TA and dl_ta_predict:
                    ta_signal = dl_ta_predict(df["price"])
                else:
                    rsi = compute_rsi(df["price"]).iloc[-1]
                    ta_signal = {
                        "symbol": SYMBOL,
                        "indicator": "RSI",
                        "value": float(rsi),
                        "timestamp": datetime.utcnow().isoformat() + "Z"
                    }
                channel.basic_publish(
                    exchange='',
                    routing_key=QUEUE_NAME,
                    body=json.dumps(ta_signal),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                logger.info(f"Published TA signal: {ta_signal}")
            time.sleep(60)
        except Exception as e:
            logger.error(f"TA loop error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped.") 