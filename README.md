# Kafka Avro Order Processing Assignment

This project demonstrates a Kafka-based order processing system for the Big Data and Analytics assignment.

## Requirements covered

- Avro-serialized order messages using `schemas/order.avsc`
- A Kafka producer that sends randomized orders
- A consumer that calculates and prints the running average price in real time
- Retry handling for temporary failures (two retries)
- A Dead Letter Queue (DLQ) for permanently failed messages
- Docker Compose configuration for Kafka and Confluent Schema Registry

## Prerequisites

- Docker Desktop running
- Python 3.10 or newer
- Git (for repository submission)

## Setup

In PowerShell, from this project folder:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose up -d
python -m app.create_topics
```

Check the containers if Kafka is not ready yet:

```powershell
docker compose ps
```

## Run the live demonstration

Open three PowerShell terminals in this folder and activate `.venv` in each one.

**Terminal 1 - run the main consumer**

```powershell
python -m app.consumer
```

**Terminal 2 - send normal orders plus failure examples**

```powershell
python -m app.producer --count 10 --include-failures
```

The consumer will:

1. Print each successful order and its continuously updated average price.
2. Retry the temporary-failure order twice, then process it successfully.
3. Send the permanent-failure order to `orders-dlq`.

**Terminal 3 - view the DLQ message**

```powershell
python -m app.dlq_consumer
```

## Kafka topics

| Topic | Purpose |
| --- | --- |
| `orders` | New order messages from the producer |
| `orders-retry` | Orders that had a temporary failure and must be retried |
| `orders-dlq` | Orders that failed permanently, with error metadata in headers |

## Notes for your presentation

Explain that `failure_mode` and `retry_count` are Kafka headers used only to demonstrate failure scenarios. The Avro value always remains the required three-field order schema: `orderId`, `product`, and `price`.

To stop and remove local Kafka containers after the demo:

```powershell
docker compose down
```
