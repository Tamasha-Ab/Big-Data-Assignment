"""Small inspection tool for the Dead Letter Queue demonstration."""

from confluent_kafka import DeserializingConsumer

from app.config import BOOTSTRAP_SERVERS, DLQ_TOPIC
from app.kafka_helpers import KEY_DESERIALIZER, avro_deserializer, decode_headers


def main() -> None:
    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "dlq-inspector",
            "auto.offset.reset": "earliest",
            "key.deserializer": KEY_DESERIALIZER,
            "value.deserializer": avro_deserializer(),
        }
    )
    consumer.subscribe([DLQ_TOPIC])
    print("Waiting for a DLQ message. Press Ctrl+C to stop.")
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"DLQ consumer error: {message.error()}")
                continue
            print(f"DLQ order: {message.value()} | metadata: {decode_headers(message.headers())}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
