import json
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika
import pytest

from app.core.config import settings
from app.core.rabbitmq import (
    check_rabbitmq_health,
    publish_event,
    setup_rabbitmq_topology,
)
from app.schemas import BookingEventPayload, OutboxEventMessage


@pytest.mark.asyncio
async def test_setup_rabbitmq_topology():
    """Проверка декларации топологии: DLX, DLQ, основного Exchange и основной очереди."""
    mock_channel = AsyncMock()

    mock_dlx = AsyncMock()
    mock_dlq = AsyncMock()
    mock_events_exchange = AsyncMock()
    mock_notifications_queue = AsyncMock()

    mock_channel.declare_exchange.side_effect = [mock_dlx, mock_events_exchange]
    mock_channel.declare_queue.side_effect = [mock_dlq, mock_notifications_queue]

    result = await setup_rabbitmq_topology(channel=mock_channel)

    # 1. Проверяем декларацию DLX
    mock_channel.declare_exchange.assert_any_call(
        name=settings.RABBITMQ_DLX_EXCHANGE,
        type=aio_pika.ExchangeType.DIRECT,
        durable=True,
    )

    # 2. Проверяем декларацию DLQ и её связывание с DLX
    mock_channel.declare_queue.assert_any_call(
        name=settings.RABBITMQ_NOTIFICATIONS_DLQ,
        durable=True,
    )
    mock_dlq.bind.assert_awaited_once_with(
        exchange=mock_dlx,
        routing_key=settings.RABBITMQ_DLQ_ROUTING_KEY,
    )

    # 3. Проверяем декларацию основного Exchange
    mock_channel.declare_exchange.assert_any_call(
        name=settings.RABBITMQ_EVENTS_EXCHANGE,
        type=aio_pika.ExchangeType.TOPIC,
        durable=True,
    )

    # 4. Проверяем параметры основной очереди с DLX
    expected_args = {
        "x-dead-letter-exchange": settings.RABBITMQ_DLX_EXCHANGE,
        "x-dead-letter-routing-key": settings.RABBITMQ_DLQ_ROUTING_KEY,
    }
    mock_channel.declare_queue.assert_any_call(
        name=settings.RABBITMQ_NOTIFICATIONS_QUEUE,
        durable=True,
        arguments=expected_args,
    )

    # 5. Проверяем привязку основной очереди к топикам событий
    mock_notifications_queue.bind.assert_awaited_once_with(
        exchange=mock_events_exchange,
        routing_key="booking.*",
    )

    assert result["dlx_exchange"] == mock_dlx
    assert result["dlq_queue"] == mock_dlq
    assert result["events_exchange"] == mock_events_exchange
    assert result["notifications_queue"] == mock_notifications_queue


@pytest.mark.asyncio
async def test_publish_event():
    """Проверка отправки события с корректными заголовками, телом и сквозным correlation_id."""
    mock_channel = AsyncMock()
    mock_exchange = AsyncMock()
    mock_channel.get_exchange.return_value = mock_exchange

    event_id = uuid.uuid4()
    booking_id = uuid.uuid4()
    master_id = uuid.uuid4()
    client_id = uuid.uuid4()
    now = datetime.now(UTC)

    payload = BookingEventPayload(
        booking_id=booking_id,
        master_id=master_id,
        client_id=client_id,
        start_at=now,
        end_at=now,
        status="CONFIRMED",
        correlation_id="corr-test-456",
    )
    event_msg = OutboxEventMessage(
        event_id=event_id,
        event_type="booking.created",
        payload=payload,
        created_at=now,
    )

    await publish_event(event=event_msg, channel=mock_channel)

    mock_channel.get_exchange.assert_awaited_once_with(settings.RABBITMQ_EVENTS_EXCHANGE)
    mock_exchange.publish.assert_awaited_once()

    published_call = mock_exchange.publish.await_args
    message: aio_pika.Message = published_call.kwargs["message"]
    routing_key: str = published_call.kwargs["routing_key"]

    assert routing_key == "booking.created"
    assert message.correlation_id == "corr-test-456"
    assert message.message_id == str(event_id)
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert message.headers["correlation_id"] == "corr-test-456"
    assert message.headers["event_type"] == "booking.created"
    assert message.headers["aggregate_id"] == str(booking_id)

    # Проверяем, что тело валидный JSON и содержит поля
    body_data = json.loads(message.body.decode("utf-8"))
    assert body_data["event_id"] == str(event_id)
    assert body_data["correlation_id"] == "corr-test-456"
    assert body_data["payload"]["booking_id"] == str(booking_id)


@pytest.mark.asyncio
async def test_check_rabbitmq_health_healthy():
    """Проверка хелсчека, когда брокер доступен."""
    mock_conn = MagicMock()
    mock_conn.is_closed = False

    with patch("app.core.rabbitmq.get_rabbitmq_connection", new_callable=AsyncMock) as mock_get_conn:
        mock_get_conn.return_value = mock_conn
        healthy = await check_rabbitmq_health()
        assert healthy is True


@pytest.mark.asyncio
async def test_check_rabbitmq_health_unhealthy():
    """Проверка хелсчека при ошибке соединения."""
    with patch("app.core.rabbitmq.get_rabbitmq_connection", new_callable=AsyncMock) as mock_get_conn:
        mock_get_conn.side_effect = ConnectionError("RabbitMQ down")
        healthy = await check_rabbitmq_health()
        assert healthy is False
