import json
import os
from datetime import datetime, timedelta

from confluent_kafka import Consumer

import streamlit as st

# Constants
KAFKA_BROKER = "broker:29092"
KAFKA_TOPIC = "rt_views_by_page"
DATA_RETENTION_MINUTES = 30

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "streamlit-consumer",
    "auto.offset.reset": "earliest",
}


# Function to get a unique key for each page and timestamp combination
def get_unique_key(data):
    return f"{data['page']}_{data['ts_win_start']}"


# Function to parse timestamp from data
def parse_timestamp(timestamp_str):
    return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")


# Function to clean up old data
def remove_old_data():
    current_time = datetime.now()
    keys_to_remove = []

    for key, data in st.session_state.messages.items():
        try:
            msg_time = parse_timestamp(data["ts_win_start"])
            if current_time - msg_time > timedelta(minutes=DATA_RETENTION_MINUTES):
                keys_to_remove.append(key)
        except (ValueError, KeyError):
            keys_to_remove.append(key)

    for key in keys_to_remove:
        del st.session_state.messages[key]

    return len(keys_to_remove)


# def get_kafka_group_id():
#     kafka_group_id = st.session_state.get("kafka_group_id", None)
#     messages = st.session_state.messages
#     if not messages:
#         messages = load_messages_from_file()
#
#     df = pd.DataFrame(messages.values())
#     earliest_timestamp = df["ts_win_start"].min()
#
#     now = datetime.now()
#     earliest_timestamp = datetime.strptime(earliest_timestamp, "%Y-%m-%d %H:%M:%S")
#
#     # If there isn't at least X minutes of data, create new group.id
#     if now - earliest_timestamp < timedelta(minutes=DATA_RETENTION_MINUTES) or not kafka_group_id:
#         # Create a new group.id for the consumer
#        st.session_state.kafka_group_id  = f"streamlit-consumer-{now.strftime('%Y%m%d%H%M%S')}"
#     elif kafka_group_id:
#         # If the group.id is still valid, use it
#         st.session_state.kafka_group_id = kafka_group_id
#
#     return st.session_state.kafka_group_id


def update_data():
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    msg_count = 0
    try:
        # Poll for messages with a timeout
        while True:
            msg = consumer.poll(5.0)
            if msg is None:
                break
            if msg.error():
                st.error(f"Kafka error: {msg.error()}")
                continue

            try:
                value_str = msg.value().decode("utf-8")
                value_str["offset"] = msg.offset()
                data = json.loads(value_str)

                # Get a unique key for this combination of page and timestamp
                unique_key = get_unique_key(data)

                # Update our dictionary with the latest data for this key
                st.session_state.messages[unique_key] = data
                msg_count += 1

            except Exception as e:
                st.error(f"Error processing message: {str(e)}")

            # Limit number of messages per refresh
            # if msg_count >= 100:
            #     break
    finally:
        consumer.close()

    # Remove outdated data
    removed = remove_old_data()

    # Update refresh timestamp
    st.session_state.last_refresh = datetime.now()

    # Save messages to file for persistence
    save_messages_to_file()

    return msg_count, removed


def save_messages_to_file():
    """Save the current messages to a file for persistence between app refreshes"""
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
    # Create data directory if it doesn't exist
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    filepath = os.path.join(data_dir, "messages.json")

    try:
        with open(filepath, "w") as f:
            # display the messages in the app while saving
            # st.json(st.session_state.messages)

            json.dump(
                # dict(st.session_state.messages),
                st.session_state.messages,
                f,
                indent=4,
            )
    except Exception as e:
        st.error(f"Error saving messages: {str(e)}")


def load_messages_from_file():
    """Load messages from file when app starts"""
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
    filepath = os.path.join(data_dir, "messages.json")

    if os.path.exists(filepath):
        try:
            with open(filepath, "rb") as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Error loading messages: {str(e)}")

    return {}
