setup:
	virtualenv -p python3 env
	./env/bin/pip install -e .[dev]

black:
	./env/bin/black --check --exclude ./tests ./s3tl ./example.py

format:
	./env/bin/black --exclude env/ ./tests ./s3tl ./example.py

mypy:
	./env/bin/mypy --exclude env/ ./tests ./s3tl ./example.py

test:
	./env/bin/pytest ./tests
