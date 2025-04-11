import json
import os
from datetime import datetime, timedelta
from typing import Dict, Union, List

import pandas as pd
from confluent_kafka import Consumer

import streamlit as st


# Function to get a unique key for each page and timestamp combination
# TODO: This is not taking into accoun the current schema
def get_unique_key(data: Dict, topic: str):
    # Handle different topics differently
    if "page" in data and "ts_win_start" in data:
        # For page view events
        return f"{topic}_{data['page']}_{data['ts_win_start']}"
    elif "artist" in data:
        # For listen events
        return (
            f"{topic}_{data['artist']}_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    else:
        # Generic fallback
        return f"{topic}_{datetime.now().strftime('%Y-%m-%d%H%M%S')}_{hash(json.dumps(data))}"


# Function to parse timestamp from data
def parse_timestamp(timestamp_str):
    return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")


# TODO:
# def streamlit_rerun(global_timer_sec: int = 0):
#     """Rerun the Streamlit app"""
#    if "timer" not in st.session_state:
#
# st.rerun()


def empty_messages(topic: str):
    """Empty the messages stored in session state and file"""
    st.session_state.messages = {}
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
    filepath = os.path.join(data_dir, f"{topic}.json")
    if os.path.exists(filepath):
        try:
            os.remove(filepath)
        except Exception as e:
            st.error(f"Error removing messages file: {str(e)}")
    st.session_state.last_refresh = datetime.now()
    st.session_state.kafka_group_id = None


# Function to clean up old data
# FIX: This function is probably wrong and not taking into account the current schema
def remove_old_data(topic: str, data_retention_minutes: Union[int, float]):
    current_time = datetime.now()
    keys_to_remove = []

    for key, data in st.session_state.messages.items():
        try:
            # Handle page view events which have ts_win_start
            if "ts_win_start" in data:
                msg_time = parse_timestamp(data["ts_win_start"])
                if current_time - msg_time > timedelta(minutes=data_retention_minutes):
                    keys_to_remove.append(key)
            # Handle artist events which have their timestamp in the key
            elif "artist" in data and "_" in key:
                topic_artist_timestamp = key.split("_")
                if len(topic_artist_timestamp) >= 3:
                    # The format should be topic_artist_YYYY-MM-DD HH:MM:SS
                    # Join all parts after the first two to handle artists with underscores
                    timestamp_str = "_".join(topic_artist_timestamp[2:])
                    try:
                        msg_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                        if current_time - msg_time > timedelta(
                            minutes=data_retention_minutes
                        ):
                            keys_to_remove.append(key)
                    except ValueError:
                        # If timestamp parsing fails, use message creation time
                        if "created_at" in data:
                            msg_time = parse_timestamp(data["created_at"])
                            if current_time - msg_time > timedelta(
                                minutes=data_retention_minutes
                            ):
                                keys_to_remove.append(key)
                        else:
                            # If no reliable timestamp, remove data older than 30 minutes by default
                            keys_to_remove.append(key)
            else:
                # For messages without a standard timestamp, remove them after retention period
                keys_to_remove.append(key)
        except (ValueError, KeyError):
            # If there's any error parsing the timestamp, remove the data
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
                data["topic"] = kafka_topic  # Add the source topic to the data

                # Get a unique key for this combination of data and topic
                unique_key = get_unique_key(data, kafka_topic)

                # Update our dictionary with the latest data for this key
                st.session_state.messages[unique_key] = data
                msg_count += 1

            except Exception as e:
                st.error(f"Error processing message: {str(e)}")

            # Limit number of messages per refresh
            if msg_count >= 1000:  # Increased limit for multiple topics
                break
    finally:
        consumer.close()

    # Remove outdated data
    removed = remove_old_data(data_retention_minutes)

    # Update refresh timestamp
    st.session_state.last_refresh = datetime.now()

    # Save messages to file for persistence
    save_messages_to_file(topic=kafka_topic)

    return msg_count, removed


def update_data_multi_topic(
    consumer_conf: Dict,
    kafka_topics: List[str],
    data_retention_minutes: Union[int, float],
):
    """Update data from multiple Kafka topics"""
    total_msg_count = 0
    total_removed = 0

    for topic in kafka_topics:
        # Create a new consumer config with a unique group ID per topic
        topic_conf = consumer_conf.copy()
        topic_conf["group.id"] = f"{topic_conf['group.id']}_{topic}"

        # Get messages for this topic
        msg_count, removed = update_data(topic_conf, topic, data_retention_minutes)

        total_msg_count += msg_count
        total_removed += removed

    return total_msg_count, total_removed


def save_messages_to_file(topic: str):
    """Save the current messages to a file for persistence between app refreshes"""
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

    # Create data directory if it doesn't exist
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    filepath = os.path.join(data_dir, f"{topic}.json")

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


def load_messages_from_file(topic: str):
    """Load messages from file when app starts"""
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
    filepath = os.path.join(data_dir, f"{topic}.json")

    if os.path.exists(filepath):
        try:
            with open(filepath, "rb") as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Error loading messages: {str(e)}")

    return {}
