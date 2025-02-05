#!/bin/bash

# Calculate start and end times
START_TIME=$(date +%Y-%m-%dT%H:%M:%S)
END_TIME=$(date -d '+1 day' +%Y-%m-%dT%H:%M:%S)

# Run the eventsim command with the calculated times
./bin/eventsim -c examples/example-config.json \
  --start-time "$START_TIME" \
  --end-time "$END_TIME" \
  --nusers 10000 \
  --growth-rate 10 \
  --randomseed 123 \
  --kafkaBrokerList broker:29092 \
  --continuous

# Log the completion of the eventsim command
echo "eventsim command completed."

# Keep the container running for debugging purposes
tail -f /dev/null