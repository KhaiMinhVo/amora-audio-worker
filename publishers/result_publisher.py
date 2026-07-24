"""
publishers/result_publisher.py
Publish kết quả vibe lên queue chat_vibe_result.
"""
import json
import pika

import config


def publish_vibe_result(payload: dict) -> None:
    params = pika.URLParameters(config.RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    try:
        channel = connection.channel()
        channel.queue_declare(queue="chat_vibe_result", durable=True)
        channel.confirm_delivery()
        confirmed = channel.basic_publish(
            exchange="",
            routing_key="chat_vibe_result",
            body=json.dumps(payload),
            mandatory=True,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
        )
        if not confirmed:
            raise RuntimeError("RabbitMQ did not confirm vibe result publication")
    finally:
        if connection.is_open:
            connection.close()
