#!/bin/bash

# Calculate start and end times
START_TIME=$(date +%Y-%m-%dT%H:%M:%S)
END_TIME=$(date -d '+1 day' +%Y-%m-%dT%H:%M:%S)

# Run the eventsim command with the calculated times
./bin/eventsim -c examples/example-config.json \
  --start-time "$START_TIME" \
  --end-time "$END_TIME" \
  --nusers 2000000 \
  --growth-rate 10 \
  --randomseed 123 \
  --kafkaBrokerList localhost:9092 \
  --continuous