#!/bin/bash
# Setup script for LocalStack

set -e

echo "Setting up LocalStack for Parquet S3 Blocks Writer"
echo "=================================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Check if LocalStack container is already running
if docker ps | grep -q localstack; then
    echo "LocalStack is already running."
    exit 0
fi

# Check if LocalStack container exists but is stopped
if docker ps -a | grep -q localstack; then
    echo "Starting existing LocalStack container..."
    docker start localstack
else
    echo "Creating and starting LocalStack container..."
    docker run --rm -d \
        -p 4566:4566 \
        -p 4510-4559:4510-4559 \
        --name localstack \
        -e SERVICES=s3 \
        -e DEBUG=1 \
        localstack/localstack
fi

echo "Waiting for LocalStack to be ready..."
sleep 5

# Check if LocalStack is healthy
for i in {1..30}; do
    if curl -s http://localhost:4566/_localstack/health > /dev/null; then
        echo "LocalStack is ready!"
        echo ""
        echo "You can now run the application with:"
        echo "  poetry run parquet-s3-writer"
        echo ""
        echo "To stop LocalStack:"
        echo "  docker stop localstack"
        exit 0
    fi
    echo "Waiting for LocalStack to be ready... (attempt $i/30)"
    sleep 2
done

echo "Error: LocalStack failed to start properly"
exit 1
