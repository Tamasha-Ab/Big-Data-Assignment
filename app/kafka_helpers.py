"""Shared Kafka, Avro, and header utilities."""

from pathlib import Path
from time import sleep

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

from app.config import BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, TOPICS


def load_order_schema() -> str:
    return Path(__file__).resolve().parents[1].joinpath("schemas", "order.avsc").read_text(
        encoding="utf-8"
    )


def schema_registry_client() -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


def avro_serializer() -> AvroSerializer:
    return AvroSerializer(schema_registry_client(), load_order_schema())


def avro_deserializer() -> AvroDeserializer:
    return AvroDeserializer(schema_registry_client())


def create_topics() -> None:
    """Create the three assignment topics once Kafka is ready.

    A single partition keeps the running average easy to demonstrate and reason about.
    """
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in TOPICS]
    futures = admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Created topic: {topic}")
        except Exception as error:  # Topic already exists is safe for repeated runs.
            if "TOPIC_ALREADY_EXISTS" not in str(error):
                raise


def wait_for_kafka(attempts: int = 15) -> None:
    """Fail with a clear message if Docker services are not ready yet."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    last_error = None
    for _ in range(attempts):
        try:
            admin.list_topics(timeout=3)
            return
        except Exception as error:
            last_error = error
            sleep(2)
    raise RuntimeError(
        "Kafka is not reachable. Run `docker compose up -d` and wait for it to start."
    ) from last_error


def decode_headers(headers: list[tuple[str, bytes | None]] | None) -> dict[str, str]:
    decoded: dict[str, str] = {}
    for key, value in headers or []:
        decoded[key] = value.decode("utf-8") if value is not None else ""
    return decoded


KEY_SERIALIZER = StringSerializer("utf_8")
KEY_DESERIALIZER = StringDeserializer("utf_8")
