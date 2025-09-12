#!/bin/bash

# Spark-Milvus Connector Job Submission Script

# Environment variables with defaults
MILVUS_URI=${MILVUS_URI:-"http://milvus-standalone.milvus-system.svc.cluster.local:19530"}
MILVUS_TOKEN=${MILVUS_TOKEN:-"root:Milvus"}
MILVUS_DATABASE=${MILVUS_DATABASE:-"default"}
MILVUS_COLLECTION=${MILVUS_COLLECTION:-"hello_spark_milvus"}

SPARK_MODE=${SPARK_MODE:-"standalone"}  # standalone or submit
SPARK_MASTER=${SPARK_MASTER:-"local[*]"}
SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-"2g"}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-"2g"}
SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-"2"}

# JAR path
CONNECTOR_JAR="/app/spark-milvus-connector.jar"

echo "Starting Spark-Milvus Connector..."
echo "================================================"
echo "Milvus URI: $MILVUS_URI"
echo "Milvus Database: $MILVUS_DATABASE"
echo "Milvus Collection: $MILVUS_COLLECTION"
echo "Spark Master: $SPARK_MASTER"
echo "Spark Mode: $SPARK_MODE"
echo "================================================"

# Export for Spark jobs
export MILVUS_URI
export MILVUS_TOKEN
export MILVUS_DATABASE
export MILVUS_COLLECTION

case "$SPARK_MODE" in
  "submit")
    # Submit a Spark job
    JOB_CLASS=${1:-"com.zilliz.spark.connector.server.ConnectorServerApp"}
    echo "Submitting Spark job: $JOB_CLASS"
    
    spark-submit \
        --master $SPARK_MASTER \
        --driver-memory $SPARK_DRIVER_MEMORY \
        --executor-memory $SPARK_EXECUTOR_MEMORY \
        --executor-cores $SPARK_EXECUTOR_CORES \
        --conf spark.sql.extensions=com.zilliz.spark.connector.extensions.VectorSearchExtensions \
        --conf spark.milvus.uri=$MILVUS_URI \
        --conf spark.milvus.token=$MILVUS_TOKEN \
        --conf spark.milvus.database=$MILVUS_DATABASE \
        --conf spark.milvus.collection=$MILVUS_COLLECTION \
        --class $JOB_CLASS \
        $CONNECTOR_JAR \
        ${@:2}
    ;;
    
  "test")
    # Run test script
    echo "Running test script..."
    spark-shell \
        --master $SPARK_MASTER \
        --driver-memory $SPARK_DRIVER_MEMORY \
        --conf spark.sql.extensions=com.zilliz.spark.connector.extensions.VectorSearchExtensions \
        --jars $CONNECTOR_JAR \
        -i /app/simple-test.scala
    ;;
    
  "verify")
    # Run verification script
    echo "Running verification script..."
    spark-shell \
        --master $SPARK_MASTER \
        --driver-memory $SPARK_DRIVER_MEMORY \
        --conf spark.sql.extensions=com.zilliz.spark.connector.extensions.VectorSearchExtensions \
        --jars $CONNECTOR_JAR \
        -i /app/verify-binlog-read.scala
    ;;
    
  "server")
    # Run the connector server
    echo "Starting Connector Server..."
    java -cp "$CONNECTOR_JAR:$SPARK_HOME/jars/*" \
        com.zilliz.spark.connector.server.ConnectorServerApp
    ;;
    
  "standalone")
    # Start Spark standalone cluster
    echo "Starting Spark standalone cluster..."
    
    # Start Spark master
    $SPARK_HOME/sbin/start-master.sh
    
    # Wait for master to start
    sleep 5
    
    # Get master URL
    MASTER_URL="spark://$(hostname -i):7077"
    echo "Spark Master started at: $MASTER_URL"
    
    # Start Spark worker
    $SPARK_HOME/sbin/start-worker.sh $MASTER_URL
    
    # Start the connector server in background
    java -cp "$CONNECTOR_JAR:$SPARK_HOME/jars/*" \
        com.zilliz.spark.connector.server.ConnectorServerApp &
    
    echo "================================================"
    echo "Spark cluster is ready!"
    echo "Spark Master UI: http://$(hostname -i):8080"
    echo "Connector Server: http://$(hostname -i):8080"
    echo "================================================"
    
    # Keep container running
    tail -f $SPARK_HOME/logs/*.out
    ;;
    
  *)
    echo "Unknown mode: $SPARK_MODE"
    echo "Available modes: standalone, submit, test, verify, server"
    exit 1
    ;;
esac