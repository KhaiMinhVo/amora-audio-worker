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
WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "").strip()

if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET must be configured.")

ALLOWED_AUDIO_HOSTS: set[str] = {
    host.strip().lower().rstrip(".")
    for host in os.getenv(
        "ALLOWED_AUDIO_HOSTS",
        "amora-voice-bucket.s3.amazonaws.com,cdn.amora.pro.vn",
    ).split(",")
    if host.strip()
}
MAX_AUDIO_BYTES: int = int(os.getenv("MAX_AUDIO_BYTES", str(20 * 1024 * 1024)))
DOWNLOAD_TIMEOUT_SECONDS: float = float(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "20"))
CHAT_MESSAGE_MAX_RETRIES: int = int(os.getenv("CHAT_MESSAGE_MAX_RETRIES", "3"))

# ── Celery Worker ─────────────────────────────────────────────────────────────
WORKER_CONCURRENCY: int = int(os.getenv("WORKER_CONCURRENCY", "2"))
