import json
import os
from typing import List, Dict
from kafka import KafkaProducer
from itertools import islice
from dotenv import load_dotenv

load_dotenv(verbose=True)


def produce_batch(topic: str, messages: List[Dict], batch_size: int = 100) -> None:
    producer = create_producer()

    try:
        for batch in create_batches(messages, batch_size):
            publish_batch(producer, topic, batch)

    except Exception as e:
        print(f"Error during Kafka batch publishing: {e}")
    finally:
        producer.close()


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def create_batches(messages: List[Dict], batch_size: int):
    iterator = iter(messages)
    while batch := list(islice(iterator, batch_size)):
        yield batch


def publish_batch(producer: KafkaProducer, topic: str, batch: List[Dict]) -> None:
    [producer.send(topic=topic, value=msg) for msg in batch]
    producer.flush()

    print(f"Published {len(batch)} messages")
    [print(f"message: {msg}") for msg in batch]
