FROM python:3.10.7-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

RUN apt update && apt install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt