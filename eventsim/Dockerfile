# # Use an official OpenJDK runtime as a parent image
# FROM openjdk:8

# # Install sbt
# RUN apt-get update && \
#     apt-get install -y curl gnupg && \
#     echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
#     echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
#     curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add && \
#     apt-get update && \
#     apt-get install -y sbt && \
#     mkdir -p /opt/eventsim

FROM ubuntu:22.04

# https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html
# The quotes on the two echo commands are necessary, for some reason.
RUN apt-get update
RUN apt-get install apt-transport-https curl gnupg -yqq
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
RUN chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk scala sbt

# Set the working directory
WORKDIR /eventsim

# Copy the necessary files into the container
# COPY eventsim/. .
# COPY ../. .
# Copy the necessary files into the container
COPY bin ./bin
COPY build ./build
COPY data ./data
COPY examples ./examples
COPY kubernetes ./kubernetes
COPY project ./project
COPY src ./src
COPY assembly.sbt ./assembly.sbt
COPY build.sbt ./build.sbt
COPY eventsim.sh ./eventsim.sh

# Run sbt assembly
RUN sbt assembly

# Copy the JAR file to the appropriate location
# RUN cp target/scala-2.12/eventsim-assembly-2.0.jar /opt/eventsim/eventsim-assembly-2.0.jar

# Make the script executable
# RUN chmod +x /eventsim/eventsim.sh
RUN chmod +x bin/eventsim
RUN sed -i 's/\r$//' bin/eventsim

CMD ["/bin/bash"]

# # Define the entrypoint to run the application
# ENTRYPOINT ["sh", "/opt/eventsim/eventsim.sh"]