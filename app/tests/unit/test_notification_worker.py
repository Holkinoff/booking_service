import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika.abc import AbstractIncomingMessage

from app.schemas import BookingEventPayload, OutboxEventMessage
from app.workers.notification_worker import NotificationWorker


@pytest.fixture
def make_event():
    def _create(
        event_type: str = "booking.created",
        client_id: uuid.UUID | None = None,
        booking_id: uuid.UUID | None = None,
        event_id: uuid.UUID | None = None,
    ) -> OutboxEventMessage:
        ev_id = event_id or uuid.uuid4()
        c_id = client_id or uuid.uuid4()
        b_id = booking_id or uuid.uuid4()
        m_id = uuid.uuid4()
        now = datetime.now(UTC)

        payload = BookingEventPayload(
            booking_id=b_id,
            master_id=m_id,
            client_id=c_id,
            start_at=now,
            end_at=now,
            status="CONFIRMED",
            correlation_id=f"test-corr-{ev_id}",
        )
        return OutboxEventMessage(
            event_id=ev_id,
            aggregate_type="booking",
            aggregate_id=b_id,
            event_type=event_type,
            payload=payload,
            correlation_id=payload.correlation_id,
            created_at=now,
        )
    return _create


def make_incoming_message(event: OutboxEventMessage) -> MagicMock:
    msg = MagicMock(spec=AbstractIncomingMessage)
    msg.body = event.model_dump_json().encode("utf-8")
    msg.content_type = "application/json"
    msg.content_encoding = "utf-8"
    msg.correlation_id = event.correlation_id
    msg.headers = {"correlation_id": event.correlation_id}
    msg.ack = AsyncMock()
    msg.reject = AsyncMock()
    return msg


@pytest.mark.asyncio
async def test_notification_worker_success(make_event):
    """Успешная обработка сообщения: отправка через канал и ACK."""
    event = make_event()
    msg = make_incoming_message(event)

    mock_sender = AsyncMock()
    mock_redis = AsyncMock()
    mock_redis.set.return_value = True  # Новое событие
    mock_redis.incr.return_value = 1   # Первое уведомление в этом часе

    worker = NotificationWorker(sender=mock_sender, redis_client=mock_redis)
    await worker.process_incoming_message(msg)

    # Канал отправки вызван
    mock_sender.send.assert_awaited_once_with(event)
    # Сообщение подтверждено в брокере
    msg.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_notification_worker_deduplication(make_event):
    """Повторное сообщение игнорируется: sender не вызывается, сообщение подтверждается (ACK)."""
    event = make_event()
    msg = make_incoming_message(event)

    mock_sender = AsyncMock()
    mock_redis = AsyncMock()
    mock_redis.set.return_value = None  # Ключ уже был в Redis (дубликат)

    worker = NotificationWorker(sender=mock_sender, redis_client=mock_redis)
    await worker.process_incoming_message(msg)

    # Канал отправки НЕ вызывался
    mock_sender.send.assert_not_called()
    # Сообщение подтверждено (ACK), чтобы убрать дубликат из очереди
    msg.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_notification_worker_rate_limiting(make_event):
    """Превышение лимита (счётчик > 3 в час): отправка блокируется, сообщение подтверждается (ACK)."""
    event = make_event()
    msg = make_incoming_message(event)

    mock_sender = AsyncMock()
    mock_redis = AsyncMock()
    mock_redis.set.return_value = True  # Новое событие
    mock_redis.incr.return_value = 4   # 4-е сообщение (лимит 3)

    worker = NotificationWorker(sender=mock_sender, redis_client=mock_redis)
    await worker.process_incoming_message(msg)

    # Канал отправки НЕ вызывался
    mock_sender.send.assert_not_called()
    msg.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_notification_worker_retries_transient_failure(make_event):
    """Сбой отправки с последующим успехом: выполняются ретраи с задержкой и сообщение подтверждается (ACK)."""
    event = make_event()
    msg = make_incoming_message(event)

    mock_sender = AsyncMock()
    # Первые две попытки падают, третья успешна
    mock_sender.send.side_effect = [
        RuntimeError("Timeout 1"),
        RuntimeError("Timeout 2"),
        None,
    ]

    mock_redis = AsyncMock()
    mock_redis.set.return_value = True
    mock_redis.incr.return_value = 1

    worker = NotificationWorker(sender=mock_sender, redis_client=mock_redis)

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await worker.process_incoming_message(msg)

    assert mock_sender.send.await_count == 3
    assert mock_sleep.await_count == 2
    msg.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_notification_worker_dead_lettering_on_max_retries(make_event):
    """При падении всех попыток ретрая сообщение отклоняется без requeue (reject(requeue=False)) -> уходит в DLQ."""
    event = make_event()
    msg = make_incoming_message(event)

    mock_sender = AsyncMock()
    mock_sender.send.side_effect = RuntimeError("Permanent failure")

    mock_redis = AsyncMock()
    mock_redis.set.return_value = True
    mock_redis.incr.return_value = 1

    worker = NotificationWorker(sender=mock_sender, redis_client=mock_redis)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        await worker.process_incoming_message(msg)

    # Сообщение отклонено с requeue=False (маршрутизируется брокером в DLQ)
    msg.reject.assert_awaited_once_with(requeue=False)
