# Build using `docker build --file=./spark/Dockerfile -t custom/spark:3.5.1-debian-12-r12 .`

FROM bitnami/spark:3.5.1-debian-12-r12

USER root

# GET JARS
WORKDIR $SPARK_HOME/jars

RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean

# RUN wget -U "My user agent" https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.41.1/spark-bigquery-with-dependencies_2.12-0.41.1.jar
# RUN wget -U "My user agent" https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.20/gcs-connector-hadoop3-2.2.20-shaded.jar
# # RUN wget -U "My user agent" https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.20/gcs-connector-hadoop3-2.2.20.jar
# RUN wget -U "My user agent" https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar
# RUN wget -U "My user agent" https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar


# COPY *.jar $SPARK_HOME/jars

# Configure GCP secret
RUN mkdir -p $SPARK_HOME/secrets
COPY ./.keys/gcp-service-account.json $SPARK_HOME/secrets/gcp-credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS=$SPARK_HOME/secrets/gcp-credentials.json 

# Configure directories
COPY ./spark/src $SPARK_HOME/src
# RUN mkdir -p $SPARK_HOME/src
# RUN chmod -R 777 $SPARK_HOME/data

WORKDIR $SPARK_HOME

# Install deps
RUN pip install delta-spark==3.0.0

# Switch bask to non-root user so nss_wrapper and nss_passwd work correctly
USER 1001