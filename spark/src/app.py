import json
import time
from datetime import datetime, timedelta

from confluent_kafka import Consumer

import streamlit as st

KAFKA_BROKER = "broker:29092"
KAFKA_TOPIC = "rt_views_by_page"

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "streamlit-consumer",
    "auto.offset.reset": "latest",
}

consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])

st.title("Kafka streaming dashboard")

# Init data storage
if "messages" not in st.session_state:
    st.session_state.messages = {}

# Create a placeholder for visualiations
table_placeholder = st.empty()  # For table
json_placeholder = st.empty()  # For JSON
raw_placeholder = st.empty()  # For raw JSON

# Consume kafka messages
while True:
    msg = consumer.poll(1)  # 1 minute
    if msg is None:
        continue  # no new messages
    if msg.error():
        st.error(f"Kafka error: {msg.error()}")
        continue

    # parse json
    try:
        data = json.loads(msg.value().decode("utf-8"))
        key = msg.key().decode("utf-8")
        unique_id = f"{key}_{data['page']}"
        st.session_state.messages[unique_id] = data

        # Get all keys older than 30 minutes and delete them
        now = datetime.now()
        for k in list(st.session_state.messages.keys()):
            message_time = datetime.strptime(
                st.session_state.messages[k]["ts_win_start"], "%Y-%m-%d %H:%M:%S"
            )
            if now - message_time > timedelta(minutes=30):
                del st.session_state.messages[k]

        # Display the messages in a table
        # table_placeholder.table(st.session_state.messages.values())
        table_placeholder.dataframe(
            sorted(
                list(st.session_state.messages.values()),
                key=lambda x: (x["page"], x["ts_win_start"]),
            ),
            use_container_width=True,
            hide_index=True,
        )

        # Display raw JSON data
        json_placeholder.json(data)

    except json.JSONDecodeError:
        st.error("Error parsing JSON")

    time.sleep(1)
