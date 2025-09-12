# Spark-Milvus Connector Docker Image
FROM openjdk:11-jdk-slim as builder

# Install build tools
RUN apt-get update && \
    apt-get install -y curl wget gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/*

# Copy source code
WORKDIR /build
COPY build.sbt ./
COPY project ./project/
COPY src ./src/

# Build the JAR
RUN sbt clean assembly

# Runtime image
FROM openjdk:11-jre-slim

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN apt-get update && \
    apt-get install -y curl wget procps && \
    wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    rm -rf /var/lib/apt/lists/*

# Set Spark environment
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:$PATH

# Create app directory
WORKDIR /app

# Copy the built JAR from builder
COPY --from=builder /build/target/scala-*/spark-milvus-assembly-*.jar /app/spark-milvus-connector.jar

# Copy test scripts
COPY simple-test.scala /app/
COPY verify-binlog-read.scala /app/

# Copy startup script
COPY spark-job.sh /app/
RUN chmod +x /app/spark-job.sh

# Expose ports
EXPOSE 4040 7077 8080 8081

# Set default command
CMD ["/app/spark-job.sh"]