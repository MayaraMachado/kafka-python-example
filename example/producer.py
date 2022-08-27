from time import sleep
from json import dumps
from kafka import KafkaProducer
import lorem
import random


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
)

for j in range(9999):
    print("Iteration", j)
    data = (
        {
            "counter": j,
            "sentence": lorem.sentence(),
            "some_value": random.choice([None, 10, 50, 12]),
        },
    )
    producer.send("topic_test", value=data),
    sleep(0.5)
