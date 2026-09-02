from __future__ import annotations

import logging
import random
import signal
import time
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from confluent_kafka import Producer

from src.common import OrderEvent, env, json_bytes, wait_for_kafka


PRODUCTS = [
    ("P100", "Wireless Headphones", "Electronics", Decimal("79.99")),
    ("P101", "Smart Watch", "Electronics", Decimal("149.00")),
    ("P200", "Running Shoes", "Fashion", Decimal("64.50")),
    ("P201", "Travel Backpack", "Fashion", Decimal("42.00")),
    ("P300", "Coffee Maker", "Home", Decimal("58.75")),
    ("P301", "Desk Lamp", "Home", Decimal("31.25")),
]
REGIONS = ["North", "South", "East", "West"]
STATUSES = ["created", "paid", "paid", "paid", "shipped", "cancelled"]
DISCOUNTS = [Decimal("0"), Decimal("0.05"), Decimal("0.10"), Decimal("0.15")]


running = True


def stop(*_: object) -> None:
    global running
    running = False


def generate_order() -> OrderEvent:
    product_id, product_name, category, unit_price = random.choice(PRODUCTS)
    return OrderEvent(
        event_id=str(uuid4()),
        order_id=f"ORD-{uuid4().hex[:10].upper()}",
        customer_id=f"CUS-{random.randint(1000, 1100)}",
        product_id=product_id,
        product_name=product_name,
        category=category,
        region=random.choice(REGIONS),
        quantity=random.randint(1, 5),
        unit_price=unit_price,
        discount_pct=random.choice(DISCOUNTS),
        status=random.choice(STATUSES),
        event_time=datetime.now(timezone.utc),
    )


def delivery_report(error, message) -> None:
    if error:
        logging.error("Delivery failed: %s", error)
    else:
        logging.info(
            "Produced event to %s[%s] at offset %s",
            message.topic(),
            message.partition(),
            message.offset(),
        )


def main() -> None:
    logging.basicConfig(
        level=env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    bootstrap_servers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = env("RAW_TOPIC", "orders.raw")
    interval = float(env("EVENT_INTERVAL_SECONDS", "1.0"))
    wait_for_kafka(bootstrap_servers)

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "retail-order-producer",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "snappy",
        }
    )

    logging.info("Producing orders to %s every %.2f seconds", topic, interval)
    while running:
        order = generate_order()
        producer.produce(
            topic,
            key=order.customer_id.encode("utf-8"),
            value=json_bytes(order),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(interval)

    producer.flush(10)
    logging.info("Producer stopped")


if __name__ == "__main__":
    main()

