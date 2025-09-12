#!/bin/bash

echo "==================================================="
echo "Building and Deploying Spark-Milvus Connector"
echo "==================================================="

# Step 1: Build the connector JAR locally
echo ""
echo "[Step 1] Building connector JAR with sbt..."
if command -v sbt &> /dev/null; then
    sbt clean assembly
    
    # Find the built JAR
    JAR_PATH=$(find target -name "*assembly*.jar" -type f | head -1)
    if [ -z "$JAR_PATH" ]; then
        echo "Error: Failed to build JAR with sbt"
        exit 1
    fi
    echo "✓ JAR built successfully: $JAR_PATH"
else
    echo "Warning: sbt not found. Using Docker to build..."
    
    # Use Docker to build if sbt is not installed
    docker run -it --rm \
        -v $(pwd):/workspace \
        -w /workspace \
        hseeberger/scala-sbt:11.0.15_1.7.1_2.13.8 \
        sbt clean assembly
    
    JAR_PATH=$(find target -name "*assembly*.jar" -type f | head -1)
    if [ -z "$JAR_PATH" ]; then
        echo "Error: Failed to build JAR with Docker"
        exit 1
    fi
    echo "✓ JAR built successfully: $JAR_PATH"
fi

# Step 2: Build Docker image with the JAR
echo ""
echo "[Step 2] Building Docker image..."

# Create a simplified Dockerfile for quick deployment
cat > Dockerfile.deploy << 'EOF'
FROM apache/spark:3.5.0-python3

# Copy the built JAR
COPY target/scala-*/spark-milvus-assembly-*.jar /opt/spark/jars/spark-milvus-connector.jar

# Copy test scripts
COPY simple-test.scala /opt/spark/work-dir/
COPY verify-binlog-read.scala /opt/spark/work-dir/

# Set working directory
WORKDIR /opt/spark/work-dir

# Default command
CMD ["/opt/spark/bin/spark-shell"]
EOF

docker build -f Dockerfile.deploy -t spark-milvus-connector:latest .
echo "✓ Docker image built: spark-milvus-connector:latest"

# Step 3: Deploy to K8s
echo ""
echo "[Step 3] Deploying to Kubernetes..."

# Create K8s deployment with the custom image
cat > spark-connector-deploy.yaml << 'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: spark-milvus
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-milvus-connector
  namespace: spark-milvus
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spark-milvus-connector
  template:
    metadata:
      labels:
        app: spark-milvus-connector
    spec:
      containers:
      - name: spark
        image: spark-milvus-connector:latest
        imagePullPolicy: Never  # Use local image
        command:
        - /bin/bash
        - -c
        - |
          echo "Spark-Milvus Connector is ready"
          echo "JAR is available at: /opt/spark/jars/spark-milvus-connector.jar"
          
          # Keep container running
          tail -f /dev/null
        env:
        - name: MILVUS_URI
          value: "http://milvus-standalone.milvus-system.svc.cluster.local:19530"
        - name: MILVUS_TOKEN
          value: "root:Milvus"
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1"
---
apiVersion: v1
kind: Service
metadata:
  name: spark-milvus-connector
  namespace: spark-milvus
spec:
  selector:
    app: spark-milvus-connector
  ports:
  - port: 4040
    name: spark-ui
EOF

kubectl apply -f spark-connector-deploy.yaml

# Step 4: Submit a test job
echo ""
echo "[Step 4] Creating test job..."

cat > test-connector-job.yaml << 'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: test-milvus-connector
  namespace: spark-milvus
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: spark
        image: spark-milvus-connector:latest
        imagePullPolicy: Never
        command:
        - /opt/spark/bin/spark-shell
        - --master
        - local[*]
        - --conf
        - spark.sql.extensions=com.zilliz.spark.connector.extensions.VectorSearchExtensions
        - -i
        - /opt/spark/work-dir/simple-test.scala
        env:
        - name: MILVUS_URI
          value: "http://milvus-standalone.milvus-system.svc.cluster.local:19530"
        - name: MILVUS_TOKEN
          value: "root:Milvus"
EOF

kubectl apply -f test-connector-job.yaml

echo ""
echo "==================================================="
echo "Deployment complete!"
echo ""
echo "To check the test job:"
echo "  kubectl logs -n spark-milvus job/test-milvus-connector"
echo ""
echo "To run interactive Spark shell:"
echo "  kubectl exec -it deployment/spark-milvus-connector -n spark-milvus -- /opt/spark/bin/spark-shell"
echo "==================================================="