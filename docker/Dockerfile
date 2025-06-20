FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies dengan Java yang benar
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    default-jdk \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME untuk Spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY data/ ./data/

# Create directories
RUN mkdir -p data/processed data/models

# Environment variables untuk Spark
ENV SPARK_LOCAL_IP=127.0.0.1
ENV MINIO_ENDPOINT=minio:9000
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Expose the port the app runs on
EXPOSE 5000 8501

# Command to run the application
CMD ["python", "src/app.py"]