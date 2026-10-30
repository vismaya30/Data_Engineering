-include .env
export

START ?= 2026-10-01
END ?= $(START)

.PHONY: install lint format test run dbt dashboard up down

install:
	pip install -r requirements.txt -r requirements-dev.txt

lint:
	ruff check .
	ruff format --check .

format:
	ruff format .
	ruff check --fix .

test:
	pytest -q

run:
	python -m src.pipeline --start-date $(START) --end-date $(END)

dbt:
	cd dbt && dbt build --profiles-dir .

dashboard:
	streamlit run dashboard/app.py

up:
	docker compose up -d --build

down:
	docker compose down
