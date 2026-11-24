import json
import time

from kafka import KafkaConsumer, TopicPartition

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "nyc311_requests"
EXPECTED_RECORDS = 500


def _topic_partition(consumer):
    partitions = consumer.partitions_for_topic(TOPIC_NAME)
    if partitions != {0}:
        raise RuntimeError(
            f"Expected Kafka topic {TOPIC_NAME!r} with one partition (0), "
            f"but found: {partitions}. Create the topic before running."
        )
    return TopicPartition(TOPIC_NAME, 0)


def get_end_offset():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
    )
    try:
        partition = _topic_partition(consumer)
        return consumer.end_offsets([partition])[partition]
    finally:
        consumer.close()


def consume_messages(expected_count=EXPECTED_RECORDS, start_offset=None, timeout_seconds=30):
    if expected_count < 1:
        raise ValueError("expected_count must be positive")

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    try:
        partition = _topic_partition(consumer)
        consumer.assign([partition])

        beginning = consumer.beginning_offsets([partition])[partition]
        end = consumer.end_offsets([partition])[partition]

        if start_offset is None:
            start_offset = max(beginning, end - expected_count)

        if not beginning <= start_offset <= end:
            raise ValueError(f"Invalid start_offset {start_offset}; valid: {beginning}..{end}")
        if end - start_offset < expected_count:
            raise RuntimeError(
                f"Only {end - start_offset} messages available starting at "
                f"offset {start_offset}; expected {expected_count}. "
                "Run python src/producer.py first."
            )

        consumer.seek(partition, start_offset)
        records = []
        deadline = time.monotonic() + timeout_seconds

        while len(records) < expected_count:
            batch = consumer.poll(
                timeout_ms=1000,
                max_records=expected_count - len(records),
            )
            for message in batch.get(partition, []):
                if not isinstance(message.value, dict):
                    raise ValueError(
                        f"Kafka message at offset {message.offset} is not a JSON object"
                    )
                records.append(message.value)

            if time.monotonic() >= deadline and len(records) < expected_count:
                raise TimeoutError(
                    f"Read {len(records)} of {expected_count} messages in "
                    f"{timeout_seconds} seconds"
                )

        print(f"Kafka messages consumed: {len(records)}")
        return records
    finally:
        consumer.close()


if __name__ == "__main__":
    received = consume_messages()
    print("First record unique_key:", received[0].get("unique_key"))
