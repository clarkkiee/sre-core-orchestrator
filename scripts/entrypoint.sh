#!/bin/bash
set -e

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
while ! nc -z ${POSTGRES_HOST:-db} ${POSTGRES_PORT:-5432}; do
  sleep 1
done
echo "PostgreSQL is ready!"

# Wait for RabbitMQ
echo "Waiting for RabbitMQ..."
while ! nc -z ${RABBITMQ_HOST:-rabbitmq} ${RABBITMQ_PORT:-5672}; do
  sleep 1
done
echo "RabbitMQ is ready!"

# Run database migrations (when alembic is set up)
# echo "Running database migrations..."
# alembic upgrade head

echo "Starting application..."
exec "$@"
