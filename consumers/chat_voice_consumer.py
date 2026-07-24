"""Consume chat voice jobs, calculate DSP features, and publish results."""

import json
import os
import signal
import tempfile
import threading
import time
import urllib.parse
from types import FrameType

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


class ChatVoiceConsumer:
    """Run the blocking Pika consumer and reconnect after broker interruptions."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._connection = None
        self._channel = None
        self._connected_at: float | None = None

    def stop(self) -> None:
        """Request shutdown and wake a blocking ``start_consuming`` call."""
        self._stop_event.set()
        connection = self._connection
        channel = self._channel

        if connection is None or not connection.is_open:
            return

        def stop_consuming() -> None:
            if channel is not None and channel.is_open:
                channel.stop_consuming()

        try:
            connection.add_callback_threadsafe(stop_consuming)
        except (pika.exceptions.AMQPError, OSError):
            # A concurrent connection loss will already wake start_consuming.
            pass

    def _consume_once(self) -> None:
        connection = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
        self._connection = connection

        try:
            channel = connection.channel()
            self._channel = channel
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
            self._connected_at = time.monotonic()
            print(
                f"[chat-voice-consumer] Listening on {QUEUE_IN}",
                flush=True,
            )

            if not self._stop_event.is_set():
                channel.start_consuming()
        finally:
            self._channel = None
            self._connection = None
            if connection.is_open:
                try:
                    connection.close()
                except (pika.exceptions.AMQPError, OSError):
                    pass

    def run_forever(self) -> None:
        retry_delay = config.RABBITMQ_RETRY_INITIAL_SECONDS

        while not self._stop_event.is_set():
            self._connected_at = None
            try:
                self._consume_once()
                if not self._stop_event.is_set():
                    print(
                        "[chat-voice-consumer] Consumer stopped unexpectedly.",
                        flush=True,
                    )
            except (pika.exceptions.AMQPError, OSError) as exc:
                if not self._stop_event.is_set():
                    print(
                        "[chat-voice-consumer] RabbitMQ connection lost: "
                        f"{type(exc).__name__}: {exc}",
                        flush=True,
                    )

            if self._stop_event.is_set():
                break

            if (
                self._connected_at is not None
                and time.monotonic() - self._connected_at
                >= config.RABBITMQ_RETRY_RESET_SECONDS
            ):
                retry_delay = config.RABBITMQ_RETRY_INITIAL_SECONDS

            print(
                "[chat-voice-consumer] Reconnecting in "
                f"{retry_delay:g} seconds...",
                flush=True,
            )
            self._stop_event.wait(retry_delay)
            retry_delay = min(
                retry_delay * 2,
                config.RABBITMQ_RETRY_MAX_SECONDS,
            )

        print("[chat-voice-consumer] Stopped.", flush=True)


def _install_signal_handlers(consumer: ChatVoiceConsumer) -> None:
    def handle_signal(signum: int, _frame: FrameType | None) -> None:
        print(
            f"[chat-voice-consumer] Received signal {signum}; shutting down...",
            flush=True,
        )
        consumer.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)


def main() -> None:
    consumer = ChatVoiceConsumer()
    _install_signal_handlers(consumer)
    consumer.run_forever()


if __name__ == "__main__":
    main()
