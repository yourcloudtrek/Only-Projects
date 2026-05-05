from google.cloud import pubsub_v1
import json
import time
import random

project_id = "triple-nectar-494911-q1"
topic_id = "order-processing"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

items = ["Laptop", "Phone", "Tablet", "Monitor", "Headphones"]

for i in range(1, 101):
    price = random.randint(100, 2000)

    message = {
        "order_id": f"ORD-{i}",
        "item_name": random.choice(items),
        "price": price
    }

    category = "HIGH" if price > 1000 else "LOW"

    publisher.publish(
        topic_path,
        json.dumps(message).encode("utf-8"),
        price=str(price),          
        category=category          
    )

    print(f"Sent: {message}, category={category}")
    time.sleep(3)