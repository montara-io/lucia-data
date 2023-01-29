PROJECT := spark_endpoint
.PHONY: test venv

commands:
	@grep '^[^#[:space:]].*:' Makefile | grep -v commands

venv:
	test -d venv || ( \
		python -m venv venv; \
		source venv/bin/activate; \
		pip install pip-tools; \
	)

compile:
	rm -f *requirements*.txt
	pip-compile requirements.in --resolver=backtracking
	pip-compile requirements-dev.in

install:
	pip install -r requirements.txt -r requirements-dev.txt

lint:
	python -m pylint $(PROJECT)

test:
	python -m pytest -vvv

run:
	FLASK_DEBUG=1 flask --app src/app.py run

db:
	docker run -d --name postgres \
	-e POSTGRES_PASSWORD=postgres \
	-v ${HOME}/postgres-data/:/var/lib/postgresql/data \
	-p 5432:5432 \
	postgres

stop-db:
	docker rm -f postgres
