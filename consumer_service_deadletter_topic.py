from google.cloud import pubsub_v1
import json

project_id = "triple-nectar-494911-q1"
subscription_id = "main-subscription"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    print(f"\nReceived: {message.data}")

    try:
        data = json.loads(message.data.decode("utf-8"))
        amount = data.get("amount")

        order_id = data.get("order_id")

        
        if amount is None:
            print("❌ FAIL: NULL VALUE")
            message.nack()
            return

        if isinstance(amount, str):
            print("❌ FAIL: TYPE ERROR")
            message.nack()
            return

        if amount < 0:
            print("❌ FAIL: NEGATIVE AMOUNT")
            message.nack()
            return

        if amount > 1000000:
            print("❌ FAIL: OUT OF RANGE")
            message.nack()
            return

        print(f"✅ PROCESSED ORDER {order_id} | amount={amount}")
        message.ack()

    except Exception as e:
        print("❌ PARSING ERROR:", str(e))
        message.nack()

print("Listening...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

streaming_pull_future.result()