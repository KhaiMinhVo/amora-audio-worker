FROM python:3.11-slim

# Cài FFmpeg (bắt buộc cho ffmpeg-python)
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Chạy Celery worker, log level INFO
CMD ["sh", "-c", "celery -A tasks worker --loglevel=info --concurrency=${WORKER_CONCURRENCY:-2}"]
