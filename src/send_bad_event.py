from __future__ import annotations

from confluent_kafka import Producer

from src.common import env, json_bytes, wait_for_kafka


def main() -> None:
    bootstrap_servers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = env("RAW_TOPIC", "orders.raw")
    wait_for_kafka(bootstrap_servers)
    producer = Producer({"bootstrap.servers": bootstrap_servers, "acks": "all"})
    invalid_order = {
        "event_id": "bad-event-001",
        "order_id": "ORD-BAD001",
        "customer_id": "CUS-1001",
        "product_id": "P100",
        "product_name": "Wireless Headphones",
        "category": "Electronics",
        "region": "Unknown",
        "quantity": -2,
        "unit_price": "79.99",
        "discount_pct": "2.00",
        "status": "paid",
        "event_time": "not-a-timestamp",
    }
    producer.produce(topic, key=b"CUS-1001", value=json_bytes(invalid_order))
    if producer.flush(10):
        raise RuntimeError("Invalid test event was not delivered")
    print(f"Sent invalid event to {topic}; the processor should route it to orders.dlq")


if __name__ == "__main__":
    main()

