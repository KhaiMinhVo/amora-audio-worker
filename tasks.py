"""
tasks.py
Celery Task chính: nhận job từ RabbitMQ, xử lý audio, báo kết quả về .NET.
"""
import os
import tempfile

import boto3
import requests
from celery import Celery
from celery.utils.log import get_task_logger

import config
from services.audio_processor import clean_audio_with_ffmpeg
from services.pet_analyzer import extract_voice_vibe

# ── Khởi tạo Celery App ───────────────────────────────────────────────────────
app = Celery('amora_worker', broker=config.RABBITMQ_URL)

# Đảm bảo task name khớp với cách .NET publish (Celery protocol v1)
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_backend=None,          # Không cần lưu result (đã dùng Webhook)
    worker_concurrency=config.WORKER_CONCURRENCY,
    task_acks_late=True,          # Chỉ ACK sau khi xử lý xong, tránh mất job khi crash
    worker_prefetch_multiplier=1, # Mỗi worker chỉ giữ 1 job mỗi lúc (phù hợp tác vụ nặng)
)

logger = get_task_logger(__name__)

# ── Khởi tạo S3 Client ────────────────────────────────────────────────────────
s3 = boto3.client(
    's3',
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    region_name=config.AWS_REGION,
)


def _report_to_dotnet(payload: dict) -> None:
    """Gọi Webhook về .NET, kèm secret header để xác thực nguồn gốc."""
    try:
        requests.post(
            config.DOTNET_WEBHOOK_URL,
            json=payload,
            headers={"X-Webhook-Secret": config.WEBHOOK_SECRET},
            timeout=10,
        )
    except requests.RequestException as exc:
        logger.error(f"[Webhook] Không thể báo về .NET: {exc}")


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def process_voice_post(self, post_id: str, s3_file_key: str):
    """
    Task chính — được Celery gọi khi .NET bắn message vào queue.

    Args:
        post_id     : ID của VoicePost cần xử lý.
        s3_file_key : Key của file gốc trên S3, ví dụ "voices/uuid.m4a".
    """
    logger.info(f"[Worker] ▶ Bắt đầu xử lý Post: {post_id} | Key: {s3_file_key}")

    # Dùng tempfile để tự động dọn dẹp kể cả khi crash
    with tempfile.NamedTemporaryFile(suffix=".m4a", delete=False) as raw_f, \
         tempfile.NamedTemporaryFile(suffix=".m4a", delete=False) as clean_f:
        raw_path = raw_f.name
        clean_path = clean_f.name

    try:
        # ── Bước 1: Tải file gốc từ S3 ───────────────────────────────────────
        logger.info(f"[Worker] ↓ Downloading s3://{config.S3_BUCKET_NAME}/{s3_file_key}")
        s3.download_file(config.S3_BUCKET_NAME, s3_file_key, raw_path)

        # ── Bước 2: Lọc nhiễu & Chuẩn hóa âm lượng ──────────────────────────
        logger.info("[Worker] 🎙 Denoising & normalizing...")
        success = clean_audio_with_ffmpeg(raw_path, clean_path)
        if not success:
            raise RuntimeError("FFmpeg xử lý thất bại.")

        # ── Bước 3: Phân tích AI cho Pet ─────────────────────────────────────
        logger.info("[Worker] 🐾 Extracting voice vibe for Pet...")
        vibe_data = extract_voice_vibe(clean_path)
        logger.info(f"[Worker] Vibe data: {vibe_data}")

        # ── Bước 4: Upload file đã xử lý đè lên S3 ───────────────────────────
        # Dùng prefix "clean_" để phân biệt với file gốc
        clean_key = s3_file_key.replace("voices/", "voices/clean_", 1)
        logger.info(f"[Worker] ↑ Uploading clean audio → s3://{config.S3_BUCKET_NAME}/{clean_key}")
        s3.upload_file(
            clean_path,
            config.S3_BUCKET_NAME,
            clean_key,
            ExtraArgs={"ContentType": "audio/mp4"},
        )

        # ── Bước 5: Báo thành công về .NET ───────────────────────────────────
        clean_audio_url = f"https://{config.S3_BUCKET_NAME}.s3.amazonaws.com/{clean_key}"
        _report_to_dotnet({
            "postId": post_id,
            "status": "Success",
            "cleanAudioUrl": clean_audio_url,
            "petVibeData": vibe_data,
        })
        logger.info(f"[Worker] ✅ Hoàn tất Post: {post_id}")

    except Exception as exc:
        logger.error(f"[Worker] ❌ Lỗi khi xử lý Post {post_id}: {exc}")
        try:
            # Retry tối đa 3 lần, mỗi lần cách nhau 30 giây
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            # Đã hết lần retry → báo thất bại về .NET
            _report_to_dotnet({"postId": post_id, "status": "Failed", "error": str(exc)})

    finally:
        # Dọn dẹp file tạm
        for path in (raw_path, clean_path):
            if os.path.exists(path):
                os.remove(path)
