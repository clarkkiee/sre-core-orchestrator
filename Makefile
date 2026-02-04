dev-up-build:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build

prod-up-build:
	docker compose -f docker-compose.yml -f docker-compose.prod.yml up --build

dev-up:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

prod-up:
	docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

down:
	docker compose down

# Alembic commands
alembic-revision:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml exec api alembic revision --autogenerate -m "$(msg)"

alembic-upgrade:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml exec api alembic upgrade head

alembic-downgrade:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml exec api alembic downgrade -1

alembic-history:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml exec api alembic history

seed-admin:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml exec api python -m app.db.seeders --count $(count)

.PHONY: dev-up dev-up-build prod-up prod-up-build down alembic-revision alembic-upgrade alembic-downgrade alembic-history seed-admin
