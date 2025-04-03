FROM python:3.9-slim

WORKDIR /app

# Install required packages for system dependencies
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  build-essential \
  curl \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY ./streamlit/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create streamlit config directory and copy config
RUN mkdir -p /app/.streamlit
COPY .streamlit/config.toml /app/.streamlit/

# Copy application code
COPY streamlit /app/streamlit
COPY spark/src /app/spark/src
COPY spark/eventsim_kafka_schemas.py /app/spark/eventsim_kafka_schemas.py

ENTRYPOINT ["tail"]
CMD ["-f", "/dev/null"] 
# ENTRYPOINT ["streamlit", "run", "streamlit/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
