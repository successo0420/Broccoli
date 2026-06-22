# Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV REDIS_URL=redis://redis:6379
ENV PYTHONUNBUFFERED=1

CMD ["video-worker", "--redis-url", "$REDIS_URL"]