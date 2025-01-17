#!/bin/bash

# Define a list of schema files in JSON
SCHEMAS_KAFKA_AVRO=(
    "../schemas/page_view_events.json"
    "../schemas/auth_events.json"
    "../schemas/listen_events.json"
    "../schemas/status_change_events.json"
)

# Loop through the list and create a curl POST request to register each schema
for SCHEMA in "${SCHEMAS_KAFKA_AVRO[@]}"; do
    # Extract the topic name from the filename
    TOPIC_NAME=$(basename "$SCHEMA" .json)
    
    # Read the schema file content and escape it
    SCHEMA_CONTENT=$(cat "$SCHEMA" | jq -Rs .)
    
    # Register the schema
    curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data "{\"schema\": $SCHEMA_CONTENT}" \
        http://schema-registry:8081/subjects/${TOPIC_NAME}-value/versions
done