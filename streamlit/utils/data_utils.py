import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Union

import pandas as pd
from confluent_kafka import Consumer

import streamlit as st


# Function to get a unique key for each page and timestamp combination
def get_unique_key(data: List):
    return f"{data['page']}_{data['ts_win_start']}"


# Function to parse timestamp from data
def parse_timestamp(timestamp_str):
    return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")


# TODO:
# def streamlit_rerun(global_timer_sec: int = 0):
#     """Rerun the Streamlit app"""
#    if "timer" not in st.session_state:
#
# st.rerun()


def empty_messages():
    """Empty the messages stored in session state and file"""
    st.session_state.messages = {}
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
    filepath = os.path.join(data_dir, "messages.json")
    if os.path.exists(filepath):
        try:
            os.remove(filepath)
        except Exception as e:
            st.error(f"Error removing messages file: {str(e)}")
    st.session_state.last_refresh = datetime.now()
    st.session_state.kafka_group_id = None


# Function to clean up old data
def remove_old_data(data_retention_minutes: Union[int, float]):
    current_time = datetime.now()
    keys_to_remove = []

    for key, data in st.session_state.messages.items():
        try:
            msg_time = parse_timestamp(data["ts_win_start"])
            if current_time - msg_time > timedelta(minutes=data_retention_minutes):
                keys_to_remove.append(key)
        except (ValueError, KeyError):
            keys_to_remove.append(key)

    for key in keys_to_remove:
        del st.session_state.messages[key]

    return len(keys_to_remove)


def get_kafka_consumer_conf(
    kafka_broker: str, auto_offset_reset: str, data_retention_minutes: Union[int, float]
):
    # Get the messages first
    kafka_group_id = st.session_state.get("kafka_group_id", None)
    messages = (
        load_messages_from_file()
        if "messages" not in st.session_state
        else st.session_state.messages
    )
    if not messages:
        messages = {}

    df = pd.DataFrame(messages.values())
    if not df.empty:
        earliest_timestamp = df["ts_win_start"].min()
        earliest_timestamp = datetime.strptime(earliest_timestamp, "%Y-%m-%d %H:%M:%S")
    else:
        earliest_timestamp = None

    now = datetime.now()

    if not kafka_group_id:
        st.session_state.kafka_group_id = (
            f"streamlit-consumer-{now.strftime('%Y%m%d%H%M%S')}"
        )

    if not earliest_timestamp:
        st.session_state.kafka_group_id = (
            f"streamlit-consumer-{now.strftime('%Y%m%d%H%M%S')}"
        )
    elif now - earliest_timestamp < timedelta(minutes=data_retention_minutes):
        st.session_state.kafka_group_id = (
            f"streamlit-consumer-{now.strftime('%Y%m%d%H%M%S')}"
        )

    return {
        "bootstrap.servers": kafka_broker,
        "group.id": st.session_state.kafka_group_id,
        "auto.offset.reset": "earliest",
    }


def update_data(
    consumer_conf: Dict, kafka_topic: str, data_retention_minutes: Union[int, float]
):
    consumer = Consumer(consumer_conf)
    consumer.subscribe([kafka_topic])

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
                data = json.loads(value_str)
                data["offset"] = msg.offset()

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
    removed = remove_old_data(data_retention_minutes)

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
