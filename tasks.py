"""Celery tasks for processing voice posts and reporting results to .NET."""

import os
import tempfile

import boto3
import requests
from celery import Celery
from celery.utils.log import get_task_logger

import config
from services.audio_processor import clean_audio_with_ffmpeg

app = Celery("amora_worker", broker=config.RABBITMQ_URL)
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_backend=None,
    worker_concurrency=config.WORKER_CONCURRENCY,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
)

logger = get_task_logger(__name__)
s3 = boto3.client(
    "s3",
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    region_name=config.AWS_REGION,
)


def _report_to_dotnet(payload: dict) -> None:
    """Report a terminal state to .NET; non-2xx responses are failures."""
    logger.info("[Webhook] Calling %s", config.DOTNET_WEBHOOK_URL)
    response = requests.post(
        config.DOTNET_WEBHOOK_URL,
        json=payload,
        headers={"X-Webhook-Secret": config.WEBHOOK_SECRET},
        timeout=10,
    )
    logger.info("[Webhook] HTTP %s", response.status_code)
    response.raise_for_status()


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def process_voice_post(self, post_id: str, s3_file_key: str):
    logger.info("[Worker] Processing Post=%s Key=%s", post_id, s3_file_key)

    with tempfile.NamedTemporaryFile(suffix=".m4a", delete=False) as raw_file, \
         tempfile.NamedTemporaryFile(suffix=".m4a", delete=False) as clean_file:
        raw_path = raw_file.name
        clean_path = clean_file.name

    try:
        metadata = s3.head_object(Bucket=config.S3_BUCKET_NAME, Key=s3_file_key)
        if int(metadata.get("ContentLength", 0)) > config.MAX_AUDIO_BYTES:
            raise ValueError("Audio file is too large")
        s3.download_file(config.S3_BUCKET_NAME, s3_file_key, raw_path)

        if not clean_audio_with_ffmpeg(raw_path, clean_path):
            raise RuntimeError("FFmpeg audio processing failed")

        if s3_file_key.startswith("voices/"):
            clean_key = s3_file_key.replace("voices/", "voices/clean_", 1)
        else:
            clean_key = f"voices/clean_{os.path.basename(s3_file_key)}"

        s3.upload_file(
            clean_path,
            config.S3_BUCKET_NAME,
            clean_key,
            ExtraArgs={"ContentType": "audio/mp4"},
        )

        clean_audio_url = f"https://{config.S3_BUCKET_NAME}.s3.amazonaws.com/{clean_key}"
        _report_to_dotnet(
            {
                "postId": post_id,
                "status": "Success",
                "cleanAudioUrl": clean_audio_url,
                "petVibeData": None,
            }
        )
        logger.info("[Worker] Completed Post=%s", post_id)

    except Exception as exc:
        logger.exception("[Worker] Failed Post=%s attempt=%s", post_id, self.request.retries + 1)

        if self.request.retries >= self.max_retries:
            try:
                _report_to_dotnet(
                    {
                        "postId": post_id,
                        "status": "Failed",
                        "error": "Audio processing failed after retries.",
                    }
                )
            except requests.RequestException:
                logger.exception(
                    "[Webhook] Unable to report terminal failure for Post=%s",
                    post_id,
                )
            raise

        raise self.retry(exc=exc)

    finally:
        for path in (raw_path, clean_path):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
