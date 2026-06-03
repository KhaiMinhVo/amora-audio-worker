"""
consumers/chat_voice_consumer.py
Lắng nghe chat_voice_processed — tải audio, phân tích DSP, publish chat_vibe_result.
"""
import json
import os
import tempfile
import urllib.parse

import boto3
import pika

import config
from processors.vibe_analyzer import compute_vibe_score
from publishers.result_publisher import publish_vibe_result

QUEUE_IN = "chat_voice_processed"


def _download_audio(audio_url: str, dest_path: str) -> None:
    if audio_url.startswith("http"):
        import requests

        resp = requests.get(audio_url, timeout=30)
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(resp.content)
        return

    # s3://bucket/key hoặc key tương đối
    key = audio_url
    if audio_url.startswith("s3://"):
        parsed = urllib.parse.urlparse(audio_url)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        s3 = boto3.client(
            "s3",
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            region_name=config.AWS_REGION,
        )
        s3.download_file(bucket, key, dest_path)
    else:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            region_name=config.AWS_REGION,
        )
        s3.download_file(config.S3_BUCKET_NAME, key, dest_path)


def _handle_message(body: bytes) -> None:
    msg = json.loads(body)
    correlation_id = msg["correlationId"]
    match_id = msg["matchId"]
    user_id = msg["userId"]
    audio_url = msg["audioUrl"]
    duration = float(msg.get("durationSeconds", 0))

    with tempfile.NamedTemporaryFile(suffix=".m4a", delete=False) as tmp:
        path = tmp.name

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
        if os.path.exists(path):
            os.remove(path)


def main() -> None:
    params = pika.URLParameters(config.RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_IN, durable=True)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, _props, body):
        try:
            _handle_message(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue=QUEUE_IN, on_message_callback=callback)
    print(f"[chat-voice-consumer] Listening on {QUEUE_IN}")
    channel.start_consuming()


if __name__ == "__main__":
    main()
