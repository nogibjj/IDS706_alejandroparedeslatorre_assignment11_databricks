install:
	pip install -r requirements.txt
format:
	black *.py --quiet
lint:
	ruff check *.py ./lib/*.py --quiet
test:
	python -m pytest -vv --nbval -cov=mylib -cov=main test_main.py

all: install format test lint