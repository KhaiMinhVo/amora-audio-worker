"""
config.py
Đọc toàn bộ cấu hình từ biến môi trường (.env hoặc OS env).
Không bao giờ hardcode secret key vào code.
"""
import os


# ── RabbitMQ ────────────────────────────────────────────────────────────────
RABBITMQ_URL: str = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672//")

# ── AWS S3 ───────────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION: str = os.getenv("AWS_REGION", "ap-southeast-1")
S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "amora-voice-bucket")

# ── .NET Webhook ─────────────────────────────────────────────────────────────
DOTNET_WEBHOOK_URL: str = os.getenv(
    "DOTNET_WEBHOOK_URL",
    "http://localhost:5000/api/webhooks/audio-processed"
)
WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "change-me-webhook-secret")

# ── Celery Worker ─────────────────────────────────────────────────────────────
WORKER_CONCURRENCY: int = int(os.getenv("WORKER_CONCURRENCY", "2"))
