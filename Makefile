.PHONY: venv install run clean

venv:
	python3 -m venv venv

install:
	venv/bin/pip install -r requirements.txt

run:
	venv/bin/python main.py

clean:
	find . -type d -name "__pycache__" -exec rm -r {} +