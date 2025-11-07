.PHONY: help install setup run test format clean localstack-start localstack-stop localstack-status

help:
	@echo "Available commands:"
	@echo "  make install          - Install project dependencies using Poetry"
	@echo "  make setup            - Copy .env.example to .env"
	@echo "  make run              - Run the application"
	@echo "  make test             - Run tests"
	@echo "  make format           - Format code with black"
	@echo "  make clean            - Clean generated files"
	@echo "  make localstack-start - Start LocalStack using Docker Compose"
	@echo "  make localstack-stop  - Stop LocalStack"
	@echo "  make localstack-status - Check LocalStack status"
	@echo "  make localstack-logs  - Show LocalStack logs"

install:
	poetry install

setup:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Created .env file from .env.example"; \
	else \
		echo ".env file already exists"; \
	fi

run:
	poetry run parquet-s3-writer

test:
	poetry run pytest -v

format:
	poetry run black src/ tests/

clean:
	rm -rf output/
	rm -rf dist/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete

localstack-start:
	docker-compose up -d
	@echo "Waiting for LocalStack to be ready..."
	@sleep 5
	@curl -s http://localhost:4566/_localstack/health && echo "LocalStack is ready!" || echo "LocalStack may not be ready yet"

localstack-stop:
	docker-compose down

localstack-status:
	@docker-compose ps
	@echo ""
	@echo "Health check:"
	@curl -s http://localhost:4566/_localstack/health 2>/dev/null || echo "LocalStack is not running"

localstack-logs:
	docker-compose logs -f localstack

# Combined command to setup and run everything
all: install setup localstack-start run
