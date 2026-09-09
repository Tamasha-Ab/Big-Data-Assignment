from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "order.avsc"

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

ORDERS_TOPIC = "orders"
RETRY_TOPIC = "orders-retry"
DLQ_TOPIC = "orders-dlq"
TOPICS = (ORDERS_TOPIC, RETRY_TOPIC, DLQ_TOPIC)

MAX_RETRIES = 2
RETRY_DELAY_SECONDS = 2
