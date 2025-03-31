import json
import time

# import polars as pl
# import glob
# import os
from confluent_kafka import Consumer

KAFKA_BROKER = "broker:29092"
KAFKA_TOPIC = "aggregated_topic"

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    # "group.id"
    "auto.offset.reset": "latest",
}

consumer = Consumer(consumer_conf)
consumer.subscribe(consumer)

st.title("Kafka streaming dashboard")

placeholder = st.empty()  # For live updates

# Consume kafka messages
while True:
    msg = consumer.poll(60)  # 1 minute
    if msg is None:
        continue  # no new messages
    if msg.error():
        st.error(f"Kafka error: {msg.error()}")
        continue

    # parse json
    try:
        data = json.loads(msg.value().decode("utf-8"))
        placeholder.write(data)
    except json.JSONDecodeError:
        st.error("Error parsing JSON")

    time.sleep(30)
