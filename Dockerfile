FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y tini curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mirror.py /app/
COPY templates /app/templates

VOLUME ["/config", "/data"]

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "mirror.py"]

HEALTHCHECK CMD curl -f http://localhost:8000/health  || exit 1
