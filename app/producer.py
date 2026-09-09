"""Produces Avro order messages, including predictable demo failures."""

import argparse
import random
import uuid

from confluent_kafka import SerializingProducer

from app.config import BOOTSTRAP_SERVERS, ORDERS_TOPIC
from app.kafka_helpers import KEY_SERIALIZER, avro_serializer

PRODUCTS = ("Laptop", "Headphones", "Keyboard", "Mouse", "Monitor")


def delivery_report(error, message) -> None:
    if error is not None:
        print(f"Delivery failed: {error}")
    else:
        print(f"Sent order {message.key()} to {message.topic()} [{message.partition()}]")


def build_order() -> dict[str, object]:
    return {
        "orderId": str(uuid.uuid4()),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(10, 500), 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Send Avro order messages to Kafka.")
    parser.add_argument("--count", type=int, default=10, help="Number of normal orders to send")
    parser.add_argument(
        "--include-failures",
        action="store_true",
        help="Also send one temporary and one permanent failure for the demo",
    )
    args = parser.parse_args()

    producer = SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "key.serializer": KEY_SERIALIZER,
            "value.serializer": avro_serializer(),
        }
    )

    messages: list[tuple[dict[str, object], str]] = [
        (build_order(), "none") for _ in range(args.count)
    ]
    if args.include_failures:
        messages.extend([(build_order(), "temporary"), (build_order(), "permanent")])

    for order, failure_mode in messages:
        headers = [("failure_mode", failure_mode)]
        producer.produce(
            topic=ORDERS_TOPIC,
            key=order["orderId"],
            value=order,
            headers=headers,
            on_delivery=delivery_report,
        )
        producer.poll(0)

    producer.flush()
    print(f"Finished sending {len(messages)} order message(s).")


if __name__ == "__main__":
    main()
