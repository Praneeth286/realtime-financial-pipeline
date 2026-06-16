"""Consumes trade ticks from Kafka and batch-writes them to PostgreSQL.

Run multiple instances with the same group id to scale horizontally:

    python consumer.py
"""
from __future__ import annotations

import json
import os
import time

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trades")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@localhost:5432/market")

BATCH_SIZE = 500
FLUSH_SECONDS = 1.0

DDL = """
CREATE TABLE IF NOT EXISTS trades (
    id     BIGSERIAL PRIMARY KEY,
    symbol TEXT        NOT NULL,
    price  NUMERIC     NOT NULL,
    size   INTEGER     NOT NULL,
    ts     TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON trades (symbol, ts DESC);
"""

INSERT = "INSERT INTO trades (symbol, price, size, ts) VALUES %s"


def connect():
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    return conn


def flush(conn, rows: list[tuple]) -> None:
    """Write a batch of rows in a single round trip, then commit offsets."""
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, INSERT, rows)
    conn.commit()


def main() -> None:
    conn = connect()
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="trade-writers",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        enable_auto_commit=False,       # commit only after a successful DB write
        auto_offset_reset="latest",
        max_poll_records=BATCH_SIZE,
    )

    print(f"Consuming {KAFKA_TOPIC} -> Postgres. Ctrl-C to stop.")
    buffer: list[tuple] = []
    last_flush = time.monotonic()
    written = 0
    try:
        for message in consumer:
            t = message.value
            buffer.append((t["symbol"], t["price"], t["size"], t["ts"]))

            # Backpressure: flush on size or time, then commit Kafka offsets.
            if len(buffer) >= BATCH_SIZE or (time.monotonic() - last_flush) >= FLUSH_SECONDS:
                flush(conn, buffer)
                consumer.commit()
                written += len(buffer)
                buffer.clear()
                last_flush = time.monotonic()
                if written and written % 5000 == 0:
                    print(f"persisted {written} rows")
    except KeyboardInterrupt:
        print("\nDraining buffer...")
        flush(conn, buffer)
        consumer.commit()
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
