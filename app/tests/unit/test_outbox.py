import asyncio
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models import OutboxEvent, OutboxStatus
from app.services.outbox_service import OutboxService


@pytest.fixture
def make_outbox_event():
    """Фабрика для создания тестовых Outbox событий."""
    def _create(
        status: OutboxStatus = OutboxStatus.PENDING,
        retry_count: int = 0,
        event_type: str = "booking.created",
    ) -> OutboxEvent:
        event_id = uuid.uuid4()
        booking_id = uuid.uuid4()
        master_id = uuid.uuid4()
        client_id = uuid.uuid4()
        now = datetime.now(UTC)

        return OutboxEvent(
            id=event_id,
            aggregate_type="booking",
            aggregate_id=booking_id,
            event_type=event_type,
            payload={
                "booking_id": str(booking_id),
                "master_id": str(master_id),
                "client_id": str(client_id),
                "start_at": now.isoformat(),
                "end_at": now.isoformat(),
                "status": "CONFIRMED",
                "correlation_id": "test-corr-123",
            },
            status=status,
            retry_count=retry_count,
            created_at=now,
        )
    return _create


@pytest.mark.asyncio
async def test_fetch_pending_events_filters_correctly(
    db_session: AsyncSession,
    make_outbox_event,
):
    """fetch_pending_events выбирает только PENDING события, игнорируя PROCESSED и FAILED."""
    ev_pending1 = make_outbox_event(status=OutboxStatus.PENDING)
    ev_pending2 = make_outbox_event(status=OutboxStatus.PENDING)
    ev_processed = make_outbox_event(status=OutboxStatus.PROCESSED)
    ev_failed = make_outbox_event(status=OutboxStatus.FAILED)

    db_session.add_all([ev_pending1, ev_pending2, ev_processed, ev_failed])
    await db_session.commit()

    events = await OutboxService.fetch_pending_events(session=db_session, limit=10)
    fetched_ids = {e.id for e in events}

    assert ev_pending1.id in fetched_ids
    assert ev_pending2.id in fetched_ids
    assert ev_processed.id not in fetched_ids
    assert ev_failed.id not in fetched_ids


@pytest.mark.asyncio
async def test_process_event_success_updates_status(
    db_session: AsyncSession,
    make_outbox_event,
):
    """При успешной публикации событие переводится в статус PROCESSED с отметкой processed_at."""
    event = make_outbox_event(status=OutboxStatus.PENDING)
    db_session.add(event)
    await db_session.commit()

    with patch("app.services.outbox_service.publish_event", new_callable=AsyncMock) as mock_pub:
        success = await OutboxService.process_event(session=db_session, event=event)
        assert success is True

        mock_pub.assert_awaited_once()
        published_msg = mock_pub.await_args.kwargs["event"]
        assert published_msg.event_id == event.id
        assert published_msg.correlation_id == "test-corr-123"

        # Проверяем состояние в базе данных
        await db_session.refresh(event)
        assert event.status == OutboxStatus.PROCESSED
        assert event.processed_at is not None
        assert event.retry_count == 0


@pytest.mark.asyncio
async def test_process_event_retry_increment_on_error(
    db_session: AsyncSession,
    make_outbox_event,
):
    """При ошибке публикации инкрементируется retry_count, а статус остаётся PENDING."""
    event = make_outbox_event(status=OutboxStatus.PENDING, retry_count=1)
    db_session.add(event)
    await db_session.commit()

    with patch("app.services.outbox_service.publish_event", side_effect=ConnectionError("RabbitMQ недоступен")):
        success = await OutboxService.process_event(session=db_session, event=event)
        assert success is False

        await db_session.refresh(event)
        assert event.status == OutboxStatus.PENDING
        assert event.retry_count == 2
        assert event.processed_at is None


@pytest.mark.asyncio
async def test_process_event_marked_failed_when_max_retries_exceeded(
    db_session: AsyncSession,
    make_outbox_event,
):
    """При достижении максимального числа ретраев (OUTBOX_MAX_RETRIES) событие переходит в FAILED."""
    event = make_outbox_event(status=OutboxStatus.PENDING, retry_count=settings.OUTBOX_MAX_RETRIES - 1)
    db_session.add(event)
    await db_session.commit()

    with patch("app.services.outbox_service.publish_event", side_effect=Exception("Critical broker error")):
        success = await OutboxService.process_event(session=db_session, event=event)
        assert success is False

        await db_session.refresh(event)
        assert event.status == OutboxStatus.FAILED
        assert event.retry_count == settings.OUTBOX_MAX_RETRIES


@pytest.mark.asyncio
async def test_process_pending_events_batch(
    db_session: AsyncSession,
    make_outbox_event,
):
    """process_pending_events обрабатывает батч событий и возвращает число успешных отправок."""
    ev1 = make_outbox_event(status=OutboxStatus.PENDING)
    ev2 = make_outbox_event(status=OutboxStatus.PENDING)
    db_session.add_all([ev1, ev2])
    await db_session.commit()

    with patch("app.services.outbox_service.publish_event", new_callable=AsyncMock) as mock_pub:
        processed_count = await OutboxService.process_pending_events(session=db_session, batch_size=10)
        assert processed_count == 2
        assert mock_pub.await_count == 2

        await db_session.refresh(ev1)
        await db_session.refresh(ev2)
        assert ev1.status == OutboxStatus.PROCESSED
        assert ev2.status == OutboxStatus.PROCESSED


@pytest.mark.asyncio
async def test_run_outbox_publisher_loop_stops_gracefully():
    """Воркер run_outbox_publisher корректно останавливается по stop_event."""
    stop_event = asyncio.Event()

    with patch("app.services.outbox_service.setup_rabbitmq_topology", new_callable=AsyncMock), \
         patch("app.services.outbox_service.async_session_maker") as mock_maker:

        mock_session = AsyncMock()
        mock_maker.return_value.__aenter__.return_value = mock_session

        with patch.object(OutboxService, "process_pending_events", return_value=0) as mock_process:
            task = asyncio.create_task(
                OutboxService.run_outbox_publisher(poll_interval=0.05, stop_event=stop_event)
            )

            await asyncio.sleep(0.08)
            assert mock_process.called

            # Сигнализируем об остановке
            stop_event.set()
            await asyncio.wait_for(task, timeout=1.0)
            assert task.done()
