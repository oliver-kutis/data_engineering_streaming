import json
import time

from confluent_kafka import Consumer

import streamlit as st

KAFKA_BROKER = "broker:29092"
KAFKA_TOPIC = "aggregated_topic"

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "auto.offset.reset": "latest",
}

consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])

st.title("Kafka streaming dashboard")

# Init data storage
if "messages" not in st.session_state:
    st.session_state.messages = []

# Create a placeholder for visualiations
table_placeholder = st.empty()  # For table
json_placeholder = st.empty()  # For JSON

# Consume kafka messages
while True:
    msg = consumer.poll(30)  # 1 minute
    if msg is None:
        continue  # no new messages
    if msg.error():
        st.error(f"Kafka error: {msg.error()}")
        continue

    # parse json
    try:
        data = json.loads(msg.value().decode("utf-8"))
        # store message in session state
        st.session_state.messages.append(data)
        if len(st.session_state.messages) > 100:
            st.session_state.messages.pop(0)

        # Display the last 10 messages in a table
        table_placeholder.table(st.session_state.messages[-10:])

        # Display raw JSON data
        json_placeholder.json(data)

    except json.JSONDecodeError:
        st.error("Error parsing JSON")

    time.sleep(10)
