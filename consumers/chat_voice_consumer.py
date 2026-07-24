"""Consume chat voice jobs, calculate DSP features, and publish results."""

import json
import os
import tempfile
import urllib.parse

import boto3
import pika

import config
from processors.vibe_analyzer import compute_vibe_score
from publishers.result_publisher import publish_vibe_result
from services.safe_download import download_http_audio

QUEUE_IN = "chat_voice_processed"
QUEUE_FAILED = "chat_voice_failed"


def _download_audio(audio_url: str, destination: str) -> None:
    if audio_url.startswith(("http://", "https://")):
        download_http_audio(audio_url, destination)
        return

    bucket = config.S3_BUCKET_NAME
    key = audio_url
    if audio_url.startswith("s3://"):
        parsed = urllib.parse.urlparse(audio_url)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if bucket != config.S3_BUCKET_NAME:
            raise ValueError("S3 bucket is not allowed")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
    )
    metadata = s3.head_object(Bucket=bucket, Key=key)
    if int(metadata.get("ContentLength", 0)) > config.MAX_AUDIO_BYTES:
        raise ValueError("Audio file is too large")
    s3.download_file(bucket, key, destination)


def _handle_message(body: bytes) -> None:
    message = json.loads(body)
    correlation_id = message["correlationId"]
    match_id = message["matchId"]
    user_id = message["userId"]
    audio_url = message["audioUrl"]
    duration = float(message.get("durationSeconds", 0))

    with tempfile.NamedTemporaryFile(suffix=".m4a", delete=False) as temp_file:
        path = temp_file.name

    try:
        _download_audio(audio_url, path)
        features = compute_vibe_score(path)
        publish_vibe_result(
            {
                "correlationId": correlation_id,
                "matchId": match_id,
                "userId": user_id,
                "vibeScore": features["vibeScore"],
                "energyRms": features["energyRms"],
                "pitchVariance": features["pitchVariance"],
                "speechRate": features["speechRate"],
                "jitter": features["jitter"],
                "durationSeconds": duration or features["durationSeconds"],
            }
        )
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def _republish_or_dead_letter(channel, properties, body: bytes, error: Exception) -> None:
    headers = dict(properties.headers or {})
    retry_count = int(headers.get("x-retry-count", 0))
    headers["x-last-error"] = str(error)[:500]

    if retry_count < config.CHAT_MESSAGE_MAX_RETRIES:
        headers["x-retry-count"] = retry_count + 1
        routing_key = QUEUE_IN
    else:
        routing_key = QUEUE_FAILED

    channel.basic_publish(
        exchange="",
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(
            content_type=properties.content_type or "application/json",
            delivery_mode=2,
            headers=headers,
            correlation_id=properties.correlation_id,
        ),
    )


def main() -> None:
    connection = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_IN, durable=True)
    channel.queue_declare(queue=QUEUE_FAILED, durable=True)
    channel.confirm_delivery()
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        try:
            _handle_message(body)
        except Exception as exc:
            _republish_or_dead_letter(ch, properties, body, exc)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_IN, on_message_callback=callback)
    print(f"[chat-voice-consumer] Listening on {QUEUE_IN}")
    channel.start_consuming()


if __name__ == "__main__":
    main()
