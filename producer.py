"""Publishes a stream of simulated trade ticks to a Kafka topic.

    python producer.py --symbols AAPL,MSFT,TSLA --rate 500
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trades")


def make_tick(symbol: str, last_price: float) -> dict:
    """Generate a plausible next tick using a small random walk."""
    drift = random.uniform(-0.5, 0.5)
    price = max(1.0, round(last_price + drift, 2))
    return {
        "symbol": symbol,
        "price": price,
        "size": random.randint(1, 500),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka trade-tick producer")
    parser.add_argument("--symbols", default="AAPL,MSFT,TSLA")
    parser.add_argument("--rate", type=int, default=200, help="messages per second")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    prices = {s: random.uniform(50, 400) for s in symbols}
    interval = 1.0 / max(1, args.rate)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=20,          # small batching window
        acks="all",
    )

    print(f"Producing to {KAFKA_TOPIC} @ ~{args.rate} msg/s. Ctrl-C to stop.")
    sent = 0
    try:
        while True:
            symbol = random.choice(symbols)
            tick = make_tick(symbol, prices[symbol])
            prices[symbol] = tick["price"]
            producer.send(KAFKA_TOPIC, key=symbol, value=tick)
            sent += 1
            if sent % 1000 == 0:
                print(f"sent {sent} ticks")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\nStopping. Total sent: {sent}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
