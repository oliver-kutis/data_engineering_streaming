# Build using:
# docker build --file=./spark/Dockerfile --build-arg SPARK_VERSION=3.5.1 --build-arg IMAGE_TAG=debian-12-r12 -t custom/spark:3.5.1-debian-12-r12 .

# Specify build arguments for version configuration
ARG SPARK_VERSION=3.5.1
ARG IMAGE_TAG=debian-12-r12
ARG SCALA_VERSION=2.12

FROM bitnami/spark:${SPARK_VERSION}-${IMAGE_TAG}

ARG SPARK_VERSION
ARG SCALA_VERSION
USER root

# GET JARS
WORKDIR $SPARK_HOME/jars

RUN apt-get update && \
  apt-get install -y wget && \
  apt-get clean

# TODO: Sort out the dependencies based on `spark_jars_output.txt`
# RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_$SCALA_VERSION/$SPARK_VERSION/spark-sql-kafka-0-10_$SCALA_VERSION-$SPARK_VERSION.jar
# RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/1.0.0/kafka-clients-1.0.0.jar
# RUN wget https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_$SCALA_VERSION/0.42.1/spark-bigquery-with-dependencies_$SCALA_VERSION-0.42.1.jar
# RUN wget https://repo1.maven.org/maven2/io/delta/delta-spark_$SCALA_VERSION/3.0.0/delta-spark_$SCALA_VERSION-3.0.0.jar

# COPY *.jar $SPARK_HOME/jars

# Configure GCP secret
RUN mkdir -p $SPARK_HOME/secrets
COPY ./.keys/gcp-service-account.json $SPARK_HOME/secrets/gcp-credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS=$SPARK_HOME/secrets/gcp-credentials.json 

# Configure directories
COPY ./spark/conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY ./spark/src $SPARK_HOME/src
# RUN mkdir -p $SPARK_HOME/src
# RUN chmod -R 777 $SPARK_HOME/data

WORKDIR $SPARK_HOME

# Install deps
RUN pip install delta-spark==3.0.0 streamlit confluent-kafka
# Add bin to path
ENV PATH="/.local/bin:${PATH}"

# Switch bask to non-root user so nss_wrapper and nss_passwd work correctly
USER 1001
