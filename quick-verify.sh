#!/bin/bash

echo "🚀 快速验证Milvus Binlog读取功能"
echo "=================================="

# 使用Docker进行快速验证
echo "📦 使用Docker构建并运行验证..."

# 创建临时的Dockerfile专门用于验证
cat > Dockerfile.verify << 'EOF'
# 使用预装Spark的镜像
FROM apache/spark:4.0.0-scala2.13-java17-python3-ubuntu

# 切换到root用户安装构建工具
USER root

# 安装sbt
RUN apt-get update && \
    apt-get install -y curl && \
    curl -fsSL "https://github.com/sbt/sbt/releases/download/v1.11.1/sbt-1.11.1.tgz" | \
    tar -xzf - -C /opt && \
    ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt && \
    rm -rf /var/lib/apt/lists/*

# 复制源代码
COPY . /app
WORKDIR /app

# 构建项目
RUN sbt clean package assembly && \
    cp target/scala-2.13/spark-connector-assembly-*.jar /opt/spark/jars/ && \
    ls -la /opt/spark/jars/*.jar

# 切换回spark用户
USER spark
WORKDIR /app

# 设置入口点
CMD ["/opt/spark/bin/spark-shell", "--master", "local[*]", "--conf", "spark.sql.extensions=com.zilliz.spark.connector.extensions.VectorSearchExtensions", "-i", "verify-binlog-read.scala"]
EOF

echo "🏗️  构建验证镜像..."
docker build -f Dockerfile.verify -t spark-milvus-verify .

if [ $? -eq 0 ]; then
    echo "✅ 构建成功"
    
    echo "🚀 运行验证..."
    docker run --rm --network host \
        -e MILVUS_URI="${MILVUS_URI:-http://localhost:19530}" \
        -e MILVUS_TOKEN="${MILVUS_TOKEN:-root:Milvus}" \
        -e MILVUS_COLLECTION="${MILVUS_COLLECTION:-hello_spark_milvus}" \
        spark-milvus-verify
        
    echo "🎉 验证完成"
else
    echo "❌ 构建失败"
    exit 1
fi

# 清理临时文件
rm -f Dockerfile.verify