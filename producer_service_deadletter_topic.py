from google.cloud import pubsub_v1
import json
import time
import random

project_id = "triple-nectar-494911-q1"
topic_id = "main-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# ❌ Different failure scenarios (DLQ triggers)
error_types = [
    {"amount": "INVALID"},
    {"amount": None},
    {"amount": "ERROR_STRING"},
    {"amount": -1},
    {"amount": 9999999999},  # unrealistic value
]

def generate_message(i):
    order_id = f"O{1000 + i}"

    # 70% good data, 30% bad data
    if random.random() < 0.7:
        return {
            "order_id": order_id,
            "amount": random.randint(100, 5000)
        }
    else:
        error_case = random.choice(error_types)
        return {
            "order_id": order_id,
            **error_case
        }

i = 1

while True:
    msg = generate_message(i)
    data = json.dumps(msg).encode("utf-8")

    publisher.publish(topic_path, data=data)
    print("Published:", msg)

    i += 1
    time.sleep(1)