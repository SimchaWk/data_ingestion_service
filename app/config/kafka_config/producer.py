import json
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv(verbose=True)


def produce(topic: str, key, value):
    producer = KafkaProducer(
        bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    try:
        producer.send(
            topic=topic,
            key=key.encode('utf-8'),
            value=value
        )
        producer.flush()
    except Exception as e:
        print(e)
