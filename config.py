"""
config.py
Đọc toàn bộ cấu hình từ biến môi trường (.env hoặc OS env).
Không bao giờ hardcode secret key vào code.
"""
import math
import os


def _read_float(name: str, default: str, minimum: float) -> float:
    raw_value = os.getenv(name, default)
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number, got {raw_value!r}.") from exc

    if not math.isfinite(value) or value < minimum:
        raise RuntimeError(
            f"{name} must be a finite number greater than or equal to {minimum}."
        )
    return value


# ── RabbitMQ ────────────────────────────────────────────────────────────────
RABBITMQ_URL: str = os.getenv(
    "RABBITMQ_URL",
    "amqp://guest:guest@localhost:5672/%2F",
)
RABBITMQ_RETRY_INITIAL_SECONDS: float = _read_float(
    "RABBITMQ_RETRY_INITIAL_SECONDS",
    "2",
    0.1,
)
RABBITMQ_RETRY_MAX_SECONDS: float = _read_float(
    "RABBITMQ_RETRY_MAX_SECONDS",
    "30",
    RABBITMQ_RETRY_INITIAL_SECONDS,
)
RABBITMQ_RETRY_RESET_SECONDS: float = _read_float(
    "RABBITMQ_RETRY_RESET_SECONDS",
    "60",
    0,
)

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
