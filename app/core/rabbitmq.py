from typing import Any

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractQueue,
    AbstractRobustConnection,
)

from app.core.config import settings
from app.core.logging import correlation_id_ctx, logger
from app.schemas import OutboxEventMessage

# Глобальное кэшированное подключение и канал
_rabbitmq_connection: AbstractRobustConnection | None = None
_rabbitmq_channel: AbstractChannel | None = None


async def get_rabbitmq_connection() -> AbstractRobustConnection:
    """
    Получение или создание устойчивого (robust) подключения к RabbitMQ.
    aio-pika автоматически восстанавливает соединение при сбоях сети или перезапуске брокера.
    """
    global _rabbitmq_connection
    if _rabbitmq_connection is None or _rabbitmq_connection.is_closed:
        _rabbitmq_connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        logger.info("rabbitmq_connection_established", url=settings.RABBITMQ_URL)
    return _rabbitmq_connection


async def get_rabbitmq_channel() -> AbstractChannel:
    """
    Получение или открытие канала RabbitMQ с включенным подтверждением публикации (publisher confirms).
    """
    global _rabbitmq_channel
    if _rabbitmq_channel is None or _rabbitmq_channel.is_closed:
        conn = await get_rabbitmq_connection()
        # publisher_confirms=True гарантирует, что брокер подтвердил получение сообщения
        _rabbitmq_channel = await conn.channel(publisher_confirms=True)
        logger.info("rabbitmq_channel_opened", publisher_confirms=True)
    return _rabbitmq_channel


async def close_rabbitmq_connection() -> None:
    """Корректное завершение соединения и канала брокера."""
    global _rabbitmq_connection, _rabbitmq_channel
    if _rabbitmq_channel is not None and not _rabbitmq_channel.is_closed:
        await _rabbitmq_channel.close()
        _rabbitmq_channel = None

    if _rabbitmq_connection is not None and not _rabbitmq_connection.is_closed:
        await _rabbitmq_connection.close()
        _rabbitmq_connection = None
        logger.info("rabbitmq_connection_closed")


async def setup_rabbitmq_topology(
    channel: AbstractChannel | None = None,
) -> dict[str, Any]:
    """
    Декларация топологии RabbitMQ:
    1. Dead Letter Exchange (DLX) и Dead Letter Queue (DLQ) для сообщений, превысивших лимит ретраев.
    2. Основной Exchange событий (Topic).
    3. Основная очередь уведомлений с привязкой к DLX (x-dead-letter-exchange).
    4. Связывание очереди с Exchange по маске событий (booking.*).
    """
    ch = channel or await get_rabbitmq_channel()

    # 1. Декларация Dead Letter Exchange (DLX)
    dlx_exchange: AbstractExchange = await ch.declare_exchange(
        name=settings.RABBITMQ_DLX_EXCHANGE,
        type=aio_pika.ExchangeType.DIRECT,
        durable=True,
    )

    # 2. Декларация Dead Letter Queue (DLQ) и связывание с DLX
    dlq_queue: AbstractQueue = await ch.declare_queue(
        name=settings.RABBITMQ_NOTIFICATIONS_DLQ,
        durable=True,
    )
    await dlq_queue.bind(
        exchange=dlx_exchange,
        routing_key=settings.RABBITMQ_DLQ_ROUTING_KEY,
    )

    # 3. Декларация основного Exchange событий
    events_exchange: AbstractExchange = await ch.declare_exchange(
        name=settings.RABBITMQ_EVENTS_EXCHANGE,
        type=aio_pika.ExchangeType.TOPIC,
        durable=True,
    )

    # 4. Декларация основной очереди уведомлений с пересылкой в DLX при ошибках/отклонении
    notifications_queue_arguments = {
        "x-dead-letter-exchange": settings.RABBITMQ_DLX_EXCHANGE,
        "x-dead-letter-routing-key": settings.RABBITMQ_DLQ_ROUTING_KEY,
    }
    notifications_queue: AbstractQueue = await ch.declare_queue(
        name=settings.RABBITMQ_NOTIFICATIONS_QUEUE,
        durable=True,
        arguments=notifications_queue_arguments,
    )

    # 5. Связывание очереди с событиями бронирования (booking.created, booking.cancelled)
    await notifications_queue.bind(
        exchange=events_exchange,
        routing_key="booking.*",
    )

    logger.info(
        "rabbitmq_topology_initialized",
        events_exchange=settings.RABBITMQ_EVENTS_EXCHANGE,
        notifications_queue=settings.RABBITMQ_NOTIFICATIONS_QUEUE,
        dlx_exchange=settings.RABBITMQ_DLX_EXCHANGE,
        dlq_queue=settings.RABBITMQ_NOTIFICATIONS_DLQ,
    )

    return {
        "events_exchange": events_exchange,
        "notifications_queue": notifications_queue,
        "dlx_exchange": dlx_exchange,
        "dlq_queue": dlq_queue,
    }


async def publish_event(
    event: OutboxEventMessage,
    channel: AbstractChannel | None = None,
    routing_key: str | None = None,
) -> None:
    """
    Публикация события в RabbitMQ:
    - Сериализует Pydantic модель в JSON.
    - Передаёт сквозной correlation_id в AMQP-свойствах и заголовках.
    - Устанавливает персистентный режим доставки (DeliveryMode.PERSISTENT).
    - Использует publisher confirms для гарантии сохранности сообщения.
    """
    ch = channel or await get_rabbitmq_channel()
    events_exchange = await ch.get_exchange(settings.RABBITMQ_EVENTS_EXCHANGE)

    target_routing_key = routing_key or event.event_type
    corr_id = event.correlation_id or correlation_id_ctx.get()

    message = aio_pika.Message(
        body=event.model_dump_json().encode("utf-8"),
        content_type="application/json",
        content_encoding="utf-8",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        message_id=str(event.event_id),
        correlation_id=corr_id,
        headers={
            "correlation_id": corr_id,
            "event_type": event.event_type,
            "aggregate_id": str(event.aggregate_id),
            "aggregate_type": event.aggregate_type,
            "created_at": event.created_at.isoformat(),
        },
    )

    await events_exchange.publish(
        message=message,
        routing_key=target_routing_key,
    )

    logger.info(
        "rabbitmq_event_published",
        event_id=str(event.event_id),
        event_type=event.event_type,
        routing_key=target_routing_key,
        correlation_id=corr_id,
    )


async def check_rabbitmq_health() -> bool:
    """Проверка доступности RabbitMQ (для readiness probe)."""
    try:
        conn = await get_rabbitmq_connection()
        return not conn.is_closed
    except Exception as exc:  # noqa: BLE001
        logger.error("rabbitmq_health_check_failed", error=str(exc))
        return False
