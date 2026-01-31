FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mirror.py web.py /app/
COPY templates /app/templates

VOLUME ["/config", "/data"]

ENTRYPOINT ["/usr/bin/tini", "--"]
