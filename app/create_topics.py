from app.kafka_helpers import create_topics, wait_for_kafka


if __name__ == "__main__":
    wait_for_kafka()
    create_topics()
    print("Kafka topics are ready.")
