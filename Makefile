IMAGE := pspa-pipeline:dev
RUN   := docker run --rm -v $(CURDIR):/app -w /app -e PYTHONPATH=/app/src:/app/dags $(IMAGE)
export AIRFLOW_UID ?= 50000

.PHONY: build test coverage lint up down logs
build:
	docker build -f docker/Dockerfile --target dev -t $(IMAGE) .
test: build
	$(RUN) python -m pytest
coverage: build
	$(RUN) python -m pytest --cov --cov-report=term-missing
lint: build
	$(RUN) bash -c "ruff check . && ruff format --check . && mypy"
up:
	mkdir -p data logs && docker compose up -d --build
	@echo "Airflow UI: http://localhost:8085  (admin / admin)"
down:
	docker compose down
logs:
	docker compose logs -f
