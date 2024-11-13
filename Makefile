install:
	pip install -r requirements.txt
test:
	python -m pytest -vv -cov=test_main.py

all: install test