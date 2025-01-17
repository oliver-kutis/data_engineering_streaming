FROM alpine:3.14

# Install jq and curl
RUN apk add --no-cache bash jq curl

WORKDIR /schemas
# Copy schema files and registration script
COPY *.json .
COPY register-schemas.sh ../bin/register-schemas.sh

# Make the registration script executable
RUN chmod +x ../bin/register-schemas.sh

# Run the registration script
CMD ["bash", "../bin/register-schemas.sh"]