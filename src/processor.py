from __future__ import annotations

import json
import logging
import signal
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer
from pydantic import ValidationError

from src.common import OrderEvent, env, json_bytes, transform_order, wait_for_kafka


running = True


def stop(*_: object) -> None:
    global running
    running = False


def publish_and_confirm(producer: Producer, topic: str, key: bytes | None, value: bytes) -> None:
    errors: list[str] = []

    def delivered(error, _message) -> None:
        if error:
            errors.append(str(error))

    producer.produce(topic, key=key, value=value, callback=delivered)
    remaining = producer.flush(10)
    if remaining or errors:
        raise RuntimeError(f"Kafka publish failed: remaining={remaining}, errors={errors}")


def main() -> None:
    logging.basicConfig(
        level=env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    bootstrap_servers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    raw_topic = env("RAW_TOPIC", "orders.raw")
    enriched_topic = env("ENRICHED_TOPIC", "orders.enriched")
    dlq_topic = env("DLQ_TOPIC", "orders.dlq")
    wait_for_kafka(bootstrap_servers)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "order-validation-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "order-processor",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "snappy",
        }
    )
    consumer.subscribe([raw_topic])
    logging.info("Processing %s into %s", raw_topic, enriched_topic)

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
                order = OrderEvent.model_validate(payload)
                enriched = transform_order(order)
                publish_and_confirm(
                    producer, enriched_topic, message.key(), json_bytes(enriched)
                )
                logging.info("Validated event_id=%s", order.event_id)
            except (json.JSONDecodeError, UnicodeDecodeError, ValidationError) as error:
                dlq_record = {
                    "source_topic": message.topic(),
                    "source_partition": message.partition(),
                    "source_offset": message.offset(),
                    "error": str(error),
                    "original_payload": message.value().decode("utf-8", errors="replace"),
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                }
                publish_and_confirm(producer, dlq_topic, message.key(), json_bytes(dlq_record))
                logging.warning("Routed invalid record to %s: %s", dlq_topic, error)

            consumer.commit(message=message, asynchronous=False)
    finally:
        producer.flush(10)
        consumer.close()
        logging.info("Processor stopped")


if __name__ == "__main__":
    main()

