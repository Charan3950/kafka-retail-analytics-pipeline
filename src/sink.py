from __future__ import annotations

import json
import logging
import signal
import time

import psycopg
from confluent_kafka import Consumer, KafkaError

from src.common import EnrichedOrder, env, wait_for_kafka


INSERT_ORDER = """
INSERT INTO orders (
    event_id, order_id, customer_id, product_id, product_name, category,
    region, quantity, unit_price, discount_pct, gross_amount, net_amount,
    status, event_time, processed_at
) VALUES (
    %(event_id)s, %(order_id)s, %(customer_id)s, %(product_id)s,
    %(product_name)s, %(category)s, %(region)s, %(quantity)s,
    %(unit_price)s, %(discount_pct)s, %(gross_amount)s, %(net_amount)s,
    %(status)s, %(event_time)s, %(processed_at)s
)
ON CONFLICT (event_id) DO NOTHING
"""


running = True


def stop(*_: object) -> None:
    global running
    running = False


def connect_with_retry() -> psycopg.Connection:
    connection_string = (
        f"host={env('POSTGRES_HOST', 'localhost')} "
        f"port={env('POSTGRES_PORT', '5432')} "
        f"dbname={env('POSTGRES_DB', 'retail')} "
        f"user={env('POSTGRES_USER', 'kafka_user')} "
        f"password={env('POSTGRES_PASSWORD', 'kafka_password')}"
    )
    for attempt in range(1, 31):
        try:
            return psycopg.connect(connection_string)
        except psycopg.OperationalError:
            if attempt == 30:
                raise
            logging.warning("PostgreSQL not ready; retry %s/30", attempt)
            time.sleep(min(attempt, 5))
    raise RuntimeError("PostgreSQL connection retries exhausted")


def main() -> None:
    logging.basicConfig(
        level=env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    bootstrap_servers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = env("ENRICHED_TOPIC", "orders.enriched")
    wait_for_kafka(bootstrap_servers)
    connection = connect_with_retry()

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "postgres-order-sink-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    logging.info("Persisting %s events to PostgreSQL", topic)

    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logging.error("Consumer error: %s", message.error())
                continue

            try:
                payload = json.loads(message.value().decode("utf-8"))
                order = EnrichedOrder.model_validate(payload)
                with connection.transaction():
                    with connection.cursor() as cursor:
                        cursor.execute(INSERT_ORDER, order.model_dump())
                consumer.commit(message=message, asynchronous=False)
                logging.info("Stored event_id=%s", order.event_id)
            except Exception:
                logging.exception("Sink failed; event offset was not committed")
                try:
                    connection.close()
                except Exception:
                    pass
                connection = connect_with_retry()
    finally:
        consumer.close()
        connection.close()
        logging.info("Sink stopped")


if __name__ == "__main__":
    main()

