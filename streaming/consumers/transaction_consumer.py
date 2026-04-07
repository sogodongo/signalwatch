import os
import time
from typing import Callable
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv
from streaming.schema import Transaction

load_dotenv()


def create_consumer(
    topic: str = None,
    group_id: str = "signalwatch-detector",
    auto_offset_reset: str = "latest",
) -> KafkaConsumer:
    """
    Creates a configured Kafka consumer.

    group_id identifies the consumer group — all consumers with the same
    group_id share the partition load. Use different group_ids for
    independent consumers of the same topic (e.g. detector + logger).
    """
    topic = topic or os.getenv("TRANSACTIONS_TOPIC", "transactions")

    return KafkaConsumer(
        topic,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        value_deserializer=lambda v: v,
        consumer_timeout_ms=5000,
    )


def consume_transactions(
    callback: Callable[[Transaction], None],
    topic: str = None,
    group_id: str = "signalwatch-detector",
    auto_offset_reset: str = "latest",
    max_messages: int = None,
):
    """
    Reads transactions from Kafka and passes each one to the callback.

    The callback pattern keeps the consumer decoupled from processing logic —
    the window processor, logger, and any other consumer all use this
    same function with different callbacks.

    max_messages is for testing — omit it for continuous production consumption.
    """
    consumer = create_consumer(topic, group_id, auto_offset_reset)
    topic_name = topic or os.getenv("TRANSACTIONS_TOPIC", "transactions")
    count = 0

    print(f"[consumer] Listening on topic '{topic_name}' "
          f"(group={group_id}, reset={auto_offset_reset})")

    try:
        for message in consumer:
            try:
                tx = Transaction.from_kafka_bytes(message.value)
                callback(tx)
                count += 1

                if max_messages and count >= max_messages:
                    print(f"[consumer] Reached limit of {max_messages} messages.")
                    break

            except Exception as e:
                # Log bad messages but keep consuming — one bad message
                # should never stop the entire pipeline
                print(f"[consumer] Failed to process message "
                      f"offset={message.offset}: {e}")

    except KafkaError as e:
        print(f"[consumer] Kafka error: {e}")
    finally:
        consumer.close()
        print(f"[consumer] Closed. Total processed: {count}")
