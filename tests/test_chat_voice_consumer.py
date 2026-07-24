"""Unit tests for RabbitMQ reconnect and shutdown behavior."""

import importlib
import os
import sys
import time
import types
import unittest
import urllib.parse
from unittest import mock


class FakeAMQPError(Exception):
    """Stand-in for pika.exceptions.AMQPError."""


fake_pika = types.ModuleType("pika")
fake_pika.exceptions = types.SimpleNamespace(AMQPError=FakeAMQPError)
fake_pika.BasicProperties = object
fake_pika.URLParameters = lambda url: url
fake_pika.BlockingConnection = lambda _parameters: None

fake_boto3 = types.ModuleType("boto3")
fake_vibe_analyzer = types.ModuleType("processors.vibe_analyzer")
fake_vibe_analyzer.compute_vibe_score = lambda _path: {}
fake_result_publisher = types.ModuleType("publishers.result_publisher")
fake_result_publisher.publish_vibe_result = lambda _result: None
fake_safe_download = types.ModuleType("services.safe_download")
fake_safe_download.download_http_audio = lambda _url, _destination: None

os.environ.setdefault("WEBHOOK_SECRET", "unit-test-secret")
config_module = importlib.import_module("config")
with mock.patch.dict(
    sys.modules,
    {
        "boto3": fake_boto3,
        "pika": fake_pika,
        "processors.vibe_analyzer": fake_vibe_analyzer,
        "publishers.result_publisher": fake_result_publisher,
        "services.safe_download": fake_safe_download,
    },
):
    consumer_module = importlib.import_module("consumers.chat_voice_consumer")


class RecordingEvent:
    def __init__(self) -> None:
        self.stopped = False
        self.delays: list[float] = []

    def is_set(self) -> bool:
        return self.stopped

    def set(self) -> None:
        self.stopped = True

    def wait(self, delay: float) -> bool:
        self.delays.append(delay)
        return self.stopped


class ChatVoiceConsumerTests(unittest.TestCase):
    def test_default_rabbitmq_url_encodes_default_vhost(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"WEBHOOK_SECRET": "unit-test-secret"},
            clear=True,
        ):
            reloaded_config = importlib.reload(config_module)
            path = urllib.parse.urlparse(reloaded_config.RABBITMQ_URL).path
            self.assertEqual("/", urllib.parse.unquote(path.removeprefix("/")))

        importlib.reload(config_module)

    def test_invalid_retry_configuration_fails_fast(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"RABBITMQ_RETRY_INITIAL_SECONDS": "not-a-number"},
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "RABBITMQ_RETRY_INITIAL_SECONDS must be a number",
            ):
                importlib.reload(consumer_module.config)

        importlib.reload(consumer_module.config)

    def test_retries_with_exponential_backoff(self) -> None:
        consumer = consumer_module.ChatVoiceConsumer()
        stop_event = RecordingEvent()
        consumer._stop_event = stop_event
        attempts = 0

        def consume_once() -> None:
            nonlocal attempts
            attempts += 1
            if attempts <= 2:
                raise FakeAMQPError("broker unavailable")
            consumer.stop()

        consumer._consume_once = consume_once

        with (
            mock.patch.object(
                consumer_module.config,
                "RABBITMQ_RETRY_INITIAL_SECONDS",
                2,
            ),
            mock.patch.object(
                consumer_module.config,
                "RABBITMQ_RETRY_MAX_SECONDS",
                30,
            ),
            mock.patch("builtins.print"),
        ):
            consumer.run_forever()

        self.assertEqual(attempts, 3)
        self.assertEqual(stop_event.delays, [2, 4])

    def test_stable_connection_resets_backoff(self) -> None:
        consumer = consumer_module.ChatVoiceConsumer()
        stop_event = RecordingEvent()
        consumer._stop_event = stop_event
        attempts = 0

        def consume_once() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise FakeAMQPError("broker unavailable")
            if attempts == 2:
                consumer._connected_at = time.monotonic() - 120
                raise FakeAMQPError("connection dropped")
            consumer.stop()

        consumer._consume_once = consume_once

        with (
            mock.patch.object(
                consumer_module.config,
                "RABBITMQ_RETRY_INITIAL_SECONDS",
                2,
            ),
            mock.patch.object(
                consumer_module.config,
                "RABBITMQ_RETRY_MAX_SECONDS",
                30,
            ),
            mock.patch.object(
                consumer_module.config,
                "RABBITMQ_RETRY_RESET_SECONDS",
                60,
            ),
            mock.patch("builtins.print"),
        ):
            consumer.run_forever()

        self.assertEqual(stop_event.delays, [2, 2])

    def test_stop_wakes_blocking_consumer(self) -> None:
        class FakeChannel:
            is_open = True
            stopped = False

            def stop_consuming(self) -> None:
                self.stopped = True

        class FakeConnection:
            is_open = True
            callback_count = 0

            def add_callback_threadsafe(self, callback) -> None:
                self.callback_count += 1
                callback()

        consumer = consumer_module.ChatVoiceConsumer()
        consumer._channel = FakeChannel()
        consumer._connection = FakeConnection()

        consumer.stop()

        self.assertTrue(consumer._stop_event.is_set())
        self.assertTrue(consumer._channel.stopped)
        self.assertEqual(consumer._connection.callback_count, 1)

    def test_connection_is_closed_when_channel_setup_fails(self) -> None:
        class FakeConnection:
            is_open = True
            closed = False

            def channel(self):
                raise FakeAMQPError("channel unavailable")

            def close(self) -> None:
                self.closed = True
                self.is_open = False

        connection = FakeConnection()
        consumer = consumer_module.ChatVoiceConsumer()

        with mock.patch.object(
            consumer_module.pika,
            "BlockingConnection",
            return_value=connection,
        ):
            with self.assertRaises(FakeAMQPError):
                consumer._consume_once()

        self.assertTrue(connection.closed)
        self.assertIsNone(consumer._connection)
        self.assertIsNone(consumer._channel)


if __name__ == "__main__":
    unittest.main()
