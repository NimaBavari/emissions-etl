cleanup:
	isort -rc .
	autoflake -r --in-place --remove-unused-variables .
	black -l 120 .
	flake8 --max-line-length 120 . --exclude .venv
	- mypy --disable-error-code import-not-found --explicit-package-bases .
	rm -rf .mypy_cache

run:
	docker build -t etl .
	docker run -it --rm -v "$(pwd)/data:/app/data" etl