"""Consumes orders, calculates a running average, retries temporary errors, and sends permanent errors to a DLQ."""

import signal
import time

from confluent_kafka import DeserializingConsumer, SerializingProducer

from app.config import (
    BOOTSTRAP_SERVERS,
    DLQ_TOPIC,
    MAX_RETRIES,
    ORDERS_TOPIC,
    RETRY_DELAY_SECONDS,
    RETRY_TOPIC,
)
from app.kafka_helpers import (
    KEY_DESERIALIZER,
    KEY_SERIALIZER,
    avro_deserializer,
    avro_serializer,
    decode_headers,
)

running = True


def stop_consumer(*_args) -> None:
    global running
    running = False


def delivery_report(error, message) -> None:
    if error is not None:
        print(f"Could not publish {message.key()}: {error}")


def send_to_topic(producer, topic: str, order: dict, headers: dict[str, str]) -> None:
    producer.produce(
        topic=topic,
        key=order["orderId"],
        value=order,
        headers=list(headers.items()),
        on_delivery=delivery_report,
    )
    producer.flush()


def main() -> None:
    signal.signal(signal.SIGINT, stop_consumer)
    signal.signal(signal.SIGTERM, stop_consumer)

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "order-average-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "key.deserializer": KEY_DESERIALIZER,
            "value.deserializer": avro_deserializer(),
        }
    )
    producer = SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "key.serializer": KEY_SERIALIZER,
            "value.serializer": avro_serializer(),
        }
    )
    consumer.subscribe([ORDERS_TOPIC, RETRY_TOPIC])

    total_price = 0.0
    successful_orders = 0
    print("Consumer started. Press Ctrl+C to stop.")

    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"Consumer error: {message.error()}")
                continue

            order = message.value()
            headers = decode_headers(message.headers())
            failure_mode = headers.get("failure_mode", "none")
            retry_count = int(headers.get("retry_count", "0"))

            try:
                if failure_mode == "permanent":
                    raise ValueError("Simulated permanent validation failure")
                if failure_mode == "temporary" and retry_count < MAX_RETRIES:
                    raise ConnectionError("Simulated temporary service failure")

                total_price += float(order["price"])
                successful_orders += 1
                average = total_price / successful_orders
                print(
                    f"Processed {order['orderId']} | {order['product']} | "
                    f"${order['price']:.2f} | running average: ${average:.2f}"
                )

            except ConnectionError as error:
                next_retry = retry_count + 1
                retry_headers = {
                    "failure_mode": failure_mode,
                    "retry_count": str(next_retry),
                    "last_error": str(error),
                }
                print(f"Temporary failure for {order['orderId']}; retry {next_retry}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY_SECONDS)
                send_to_topic(producer, RETRY_TOPIC, order, retry_headers)

            except Exception as error:
                dlq_headers = {
                    "failure_mode": failure_mode,
                    "retry_count": str(retry_count),
                    "error": str(error),
                    "original_topic": message.topic(),
                }
                print(f"Permanent failure for {order['orderId']}; sending it to {DLQ_TOPIC}")
                send_to_topic(producer, DLQ_TOPIC, order, dlq_headers)

            finally:
                consumer.commit(message=message, asynchronous=False)
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
