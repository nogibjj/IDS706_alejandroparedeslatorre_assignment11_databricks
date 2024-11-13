install:
	pip install -r requirements.txt
test:
	python -m pytest -vv --nbval -cov=mylib -cov=main test_main.py

all: install test