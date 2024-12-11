# Base image for Python
ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
# Base image for OpenJDK
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

ARG PYSPARK_VERSION=3.2.0

# Install wget, ping, and PySpark
RUN apt-get update && \
    apt-get install -y wget iputils-ping && \
    pip --no-cache-dir install pyspark==${PYSPARK_VERSION}

# Download and add Kafka dependencies
RUN mkdir -p /opt/spark/jars && \
    wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${PYSPARK_VERSION}/spark-sql-kafka-0-10_2.12-${PYSPARK_VERSION}.jar && \
    wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${PYSPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${PYSPARK_VERSION}.jar

# Clean up APT cache to reduce image size
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment variable to include Spark jars
ENV SPARK_CLASSPATH="/opt/spark/jars/*"

WORKDIR /home

COPY pyspark/*.py /home/scripts/


# Keep the container running
CMD ["tail", "-f", "/dev/null"]
