PROJECT := lucia
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
	sed -i '' '/gitlab.com/d' requirements.txt
	pip-compile requirements-dev.in
	sed -i '' '/gitlab.com/d' requirements-dev.txt

install:
	pip install -r requirements.txt -r requirements-dev.txt

lint:
	python -m pylint $(PROJECT)

test:
	echo "HELLO"
	python -m pytest -vvv