#!/bin/bash

# Deployment script for Spark-Milvus Connector

echo "Deploying Spark-Milvus Connector to Kubernetes..."

# Build the connector JAR
echo "Building connector JAR with sbt..."
sbt clean assembly

# Build Docker image
echo "Building Docker image..."
docker build -t spark-milvus-connector:latest .

# Deploy to Kubernetes
echo "Deploying to Kubernetes..."
kubectl apply -f spark-connector.yaml

# Wait for deployment
echo "Waiting for deployment to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/spark-milvus-connector -n spark-milvus

# Get service info
echo "Deployment complete! Service information:"
kubectl get svc -n spark-milvus
kubectl get pods -n spark-milvus

echo ""
echo "To submit a Spark job, use:"
echo "kubectl exec -it <pod-name> -n spark-milvus -- /app/spark-job.sh submit <your-job-class>"
echo ""
echo "To access Spark UI, port-forward:"
echo "kubectl port-forward svc/spark-milvus-connector 4040:4040 -n spark-milvus"