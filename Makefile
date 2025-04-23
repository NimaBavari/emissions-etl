code-quality:
	isort -rc .
	autoflake -r --in-place --remove-unused-variables .
	black -l 120 .
	flake8 --max-line-length 120 . --exclude .venv
	- mypy --disable-error-code import-not-found --explicit-package-bases .
	rm -rf .mypy_cache

start:
	docker-compose up -d

stop:
	docker-compose down --remove-orphans

monitor:
	docker-compose logs --follow