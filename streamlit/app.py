from datetime import datetime

import pandas as pd
import plotly.express as px
from utils.data_utils import load_messages_from_file, parse_timestamp, update_data

import streamlit as st

KAFKA_BROKER = "broker:29092"
KAFKA_TOPIC = "rt_views_by_page"
DATA_RETENTION_MINUTES = 30

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "streamlit-consumer",
    "auto.offset.reset": "earliest",
}

st.title("Real-time Streaming Dashboard")
st.write("Displaying page view events in real-time from Kafka")

# Initialize session state variables
if "messages" not in st.session_state:
    st.session_state.messages = load_messages_from_file()
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = datetime.now()

# Create containers for our dashboard
header = st.container()
data_stats = st.container()
data_display = st.container()

with header:
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.subheader("Real-time Page View Events")
    with col2:
        if st.button("Refresh Data"):
            new_msgs, removed_msgs = update_data()
            st.success(f"Added {new_msgs}, removed {removed_msgs} messages")
    with col3:
        if st.button("Clear Data"):
            st.session_state.messages = {}

# # Function to get a unique key for each page and timestamp combination
# def get_unique_key(data):
#     return f"{data['page']}_{data['ts_win_start']}"
#
#
# # Function to parse timestamp from data
# def parse_timestamp(timestamp_str):
#     return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
#
#
# # Function to clean up old data
# def remove_old_data():
#     current_time = datetime.now()
#     keys_to_remove = []
#
#     for key, data in st.session_state.messages.items():
#         try:
#             msg_time = parse_timestamp(data["ts_win_start"])
#             if current_time - msg_time > timedelta(minutes=DATA_RETENTION_MINUTES):
#                 keys_to_remove.append(key)
#         except (ValueError, KeyError):
#             keys_to_remove.append(key)
#
#     for key in keys_to_remove:
#         del st.session_state.messages[key]
#
#     return len(keys_to_remove)
#
#
# def update_data():
#     consumer = Consumer(consumer_conf)
#     consumer.subscribe([KAFKA_TOPIC])
#
#     msg_count = 0
#     try:
#         # Poll for messages with a timeout
#         while True:
#             msg = consumer.poll(1.0)
#             if msg is None:
#                 break
#             if msg.error():
#                 st.error(f"Kafka error: {msg.error()}")
#                 continue
#
#             try:
#                 value_str = msg.value().decode("utf-8")
#                 data = json.loads(value_str)
#
#                 # Get a unique key for this combination of page and timestamp
#                 unique_key = get_unique_key(data)
#
#                 # Update our dictionary with the latest data for this key
#                 st.session_state.messages[unique_key] = data
#                 msg_count += 1
#
#             except Exception as e:
#                 st.error(f"Error processing message: {str(e)}")
#
#             # Limit number of messages per refresh
#             if msg_count >= 100:
#                 break
#     finally:
#         consumer.close()
#
#     # Remove outdated data
#     removed = remove_old_data()
#
#     # Update refresh timestamp
#     st.session_state.last_refresh = datetime.now()
#
#     return msg_count, removed
#

# Main data display loop
with data_stats:
    refresh_col, retention_col, status_col = st.columns(3)

    with refresh_col:
        auto_refresh = st.checkbox("Auto-refresh data", value=True)
        refresh_interval = st.slider("Refresh interval (sec)", 1, 30, 5)

    with retention_col:
        st.metric("Data retention", f"{DATA_RETENTION_MINUTES} minutes")

    with status_col:
        last_refresh = st.session_state.last_refresh.strftime("%H:%M:%S")
        status_placeholder = st.empty()
        status_placeholder.metric("Last update", last_refresh)

# Data tables
# Data tables
with data_display:
    tab1, tab2, tab3, tab4 = st.tabs(
        ["Data Table", "Summary by Page", "Time Series Chart", "Raw JSON"]
    )

    # Periodically refresh data
    current_time = datetime.now()
    time_since_refresh = (current_time - st.session_state.last_refresh).seconds

    # Force refresh if auto-refresh is enabled
    if auto_refresh:
        if time_since_refresh >= refresh_interval:
            new_msgs, removed_msgs = update_data()
            status_text = f"Added {new_msgs}, removed {removed_msgs} msgs"
            status_placeholder.metric(
                "Last update", current_time.strftime("%H:%M:%S"), delta=status_text
            )
            st.rerun()

    # Get current messages and sort them
    current_messages = list(st.session_state.messages.values())
    # df_current_messages = pd.DataFrame(current_messages).sort_values(
    #     ["page", "ts_win_start"], ascending=[True, True], ignore_index=True
    # )

    # Display data table
    with tab1:
        if current_messages:
            st.dataframe(
                # df_current_messages,
                sorted(
                    current_messages,
                    key=lambda x: (x.get("ts_win_start", ""), x.get("page", "")),
                ),
                use_container_width=True,
                # remove_index=True,
            )
        else:
            st.info("No messages received yet.")

    # Summary table by page
    with tab2:
        if current_messages:
            # Create a summary dictionary for page counts
            page_summary = {}
            for msg in current_messages:
                page = msg.get("page", "Unknown")
                count = msg.get("count", 0)
                if page in page_summary:
                    page_summary[page] += count
                else:
                    page_summary[page] = count

            # Convert to a DataFrame for display

            summary_df = (
                pd.DataFrame(
                    {
                        "Page": list(page_summary.keys()),
                        "Total Views": list(page_summary.values()),
                    }
                )
                .sort_values("Total Views", ascending=False)
                .style.hide(axis="index")
            )

            st.subheader("Page View Summary")
            st.dataframe(summary_df, use_container_width=True)

            # Show total views
            total_views = sum(page_summary.values())
            st.metric("Total Page Views", f"{total_views:,}")
        else:
            st.info("No messages received yet.")

    # Time series chart
    with tab3:
        if current_messages:
            # Prepare data for chart
            chart_data = []
            for msg in current_messages:
                try:
                    ts = parse_timestamp(msg.get("ts_win_start", ""))
                    chart_data.append(
                        {
                            "timestamp": ts,
                            "count": msg.get("count", 0),
                            "page": msg.get("page", "Unknown"),
                        }
                    )
                except (ValueError, KeyError):
                    pass

            if chart_data:
                # Convert to DataFrame
                chart_df = pd.DataFrame(chart_data)

                # Create minutes ago and readable time columns
                now = datetime.now()
                chart_df["minutes_ago"] = chart_df["timestamp"].apply(
                    lambda x: int((now - x).total_seconds() / 60) * -1
                )
                chart_df["time_str"] = chart_df["timestamp"].dt.strftime("%H:%M")

                # Group by minute and sum counts
                time_summary = (
                    chart_df.groupby(["minutes_ago", "time_str"])["count"]
                    .sum()
                    .reset_index()
                )
                time_summary = time_summary.sort_values("minutes_ago")

                # Create the chart
                fig = px.bar(
                    time_summary,
                    x="minutes_ago",
                    y="count",
                    text="time_str",
                    labels={
                        # "minutes_ago": "Minutes Ago",
                        # "count": "Page Views",
                        "time_str": "Time",
                    },
                    title="Page Views by Time",
                )

                # Customize the chart
                fig.update_layout(
                    xaxis_title=None,
                    yaxis_title=None,
                    xaxis={
                        "categoryorder": "category descending",
                        "ticksuffix": " min",
                    },
                )

                # Show time labels under bars
                fig.update_traces(textposition="outside", textangle=0)

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough data for chart visualization")
        else:
            st.info("No messages received yet.")

    # Display raw JSON
    with tab4:
        if current_messages:
            st.json(current_messages[-1])  # Show most recent message
        else:
            st.info("No messages received yet.")
