import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models import BookingStatus, Master, OutboxEvent, OutboxStatus
from app.schemas import BookingCreateRequest
from app.services.booking_service import BookingService


@pytest.mark.asyncio
async def test_booking_in_past_rejected(db_session: AsyncSession, test_master: Master, mock_redis):
    """Попытка записаться на время в прошлом должна отклоняться с кодом 400."""
    past_time = datetime.now(timezone.utc) - timedelta(hours=2)
    request_data = BookingCreateRequest(
        master_id=test_master.id,
        client_id=uuid.uuid4(),
        start_at=past_time,
    )

    with pytest.raises(HTTPException) as exc_info:
        await BookingService.create_booking(
            session=db_session,
            data=request_data,
            idempotency_key=str(uuid.uuid4()),
            correlation_id="test-corr-id",
        )

    assert exc_info.value.status_code == 400
    assert "прошлом" in exc_info.value.detail or f"{settings.BOOKING_MIN_MINUTES_BEFORE_SLOT}" in exc_info.value.detail


@pytest.mark.asyncio
async def test_booking_closer_than_configured_threshold_rejected(
    db_session: AsyncSession,
    test_master: Master,
    mock_redis,
):
    """Попытка записаться ближе чем за настроенное число минут отклоняется с кодом 400."""
    # Берем время ближе настроенного порога (порог - 5 минут)
    too_soon_time = datetime.now(timezone.utc) + timedelta(
        minutes=max(1, settings.BOOKING_MIN_MINUTES_BEFORE_SLOT - 5)
    )
    request_data = BookingCreateRequest(
        master_id=test_master.id,
        client_id=uuid.uuid4(),
        start_at=too_soon_time,
    )

    with pytest.raises(HTTPException) as exc_info:
        await BookingService.create_booking(
            session=db_session,
            data=request_data,
            idempotency_key=str(uuid.uuid4()),
            correlation_id="test-corr-id",
        )

    assert exc_info.value.status_code == 400
    assert f"{settings.BOOKING_MIN_MINUTES_BEFORE_SLOT} минут" in exc_info.value.detail


@pytest.mark.asyncio
async def test_booking_min_minutes_setting_is_configurable(
    db_session: AsyncSession,
    test_master: Master,
    mock_redis,
    monkeypatch,
):
    """
    Проверка, что ограничение действительно гибкое и опирается на настройку:
    Если изменить настройку на 120 минут, то 90 минут должны отклоняться, а 130 — приниматься.
    """
    custom_minutes = 120
    monkeypatch.setattr(settings, "BOOKING_MIN_MINUTES_BEFORE_SLOT", custom_minutes)

    # 1. 90 минут (< 120) должно быть отклонено
    too_soon_time = datetime.now(timezone.utc) + timedelta(minutes=90)
    with pytest.raises(HTTPException) as exc_info:
        await BookingService.create_booking(
            session=db_session,
            data=BookingCreateRequest(
                master_id=test_master.id,
                client_id=uuid.uuid4(),
                start_at=too_soon_time,
            ),
            idempotency_key=str(uuid.uuid4()),
            correlation_id="test-corr-id",
        )
    assert exc_info.value.status_code == 400
    assert f"{custom_minutes} минут" in exc_info.value.detail

    # 2. 130 минут (> 120) должно успешно создаться
    allowed_time = datetime.now(timezone.utc) + timedelta(minutes=130)
    booking_resp, status_code = await BookingService.create_booking(
        session=db_session,
        data=BookingCreateRequest(
            master_id=test_master.id,
            client_id=uuid.uuid4(),
            start_at=allowed_time,
        ),
        idempotency_key=str(uuid.uuid4()),
        correlation_id="test-corr-id",
    )
    assert status_code == 201
    assert booking_resp.status == BookingStatus.CONFIRMED


@pytest.mark.asyncio
async def test_booking_allowed_after_configured_threshold_and_creates_outbox(
    db_session: AsyncSession,
    test_master: Master,
    mock_redis,
):
    """
    Запись на слот после порога settings.BOOKING_MIN_MINUTES_BEFORE_SLOT:
    - Успешно создается со статусом CONFIRMED.
    - Автоматически создает запись в Outbox со статусом PENDING.
    - Вызывает инвалидацию кэша слотов.
    """
    future_time = datetime.now(timezone.utc) + timedelta(
        minutes=settings.BOOKING_MIN_MINUTES_BEFORE_SLOT + 30
    )
    client_id = uuid.uuid4()
    request_data = BookingCreateRequest(
        master_id=test_master.id,
        client_id=client_id,
        start_at=future_time,
    )

    booking_resp, status_code = await BookingService.create_booking(
        session=db_session,
        data=request_data,
        idempotency_key=str(uuid.uuid4()),
        correlation_id="test-corr-id",
    )

    assert status_code == 201
    assert booking_resp.master_id == test_master.id
    assert booking_resp.client_id == client_id
    assert booking_resp.status == BookingStatus.CONFIRMED

    # Проверяем, что событие записано в Outbox
    outbox_result = await db_session.execute(
        select(OutboxEvent).where(OutboxEvent.aggregate_id == booking_resp.id)
    )
    outbox_event = outbox_result.scalar_one_or_none()
    assert outbox_event is not None
    assert outbox_event.event_type == "booking.created"
    assert outbox_event.status == OutboxStatus.PENDING
    assert outbox_event.payload["booking_id"] == str(booking_resp.id)
    assert outbox_event.payload["correlation_id"] == "test-corr-id"

    # Проверяем инвалидацию кэша
    mock_redis.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_booking_inactive_master_rejected(db_session: AsyncSession, mock_redis):
    """Попытка записаться к несуществующему мастеру возвращает 404."""
    future_time = datetime.now(timezone.utc) + timedelta(hours=2)
    request_data = BookingCreateRequest(
        master_id=uuid.uuid4(),
        client_id=uuid.uuid4(),
        start_at=future_time,
    )

    with pytest.raises(HTTPException) as exc_info:
        await BookingService.create_booking(
            session=db_session,
            data=request_data,
            idempotency_key=str(uuid.uuid4()),
            correlation_id="test-corr-id",
        )

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_cancel_booking_success(db_session: AsyncSession, test_master: Master, mock_redis):
    """Отмена бронирования переводит статус в CANCELLED и создает Outbox-событие booking.cancelled."""
    future_time = datetime.now(timezone.utc) + timedelta(hours=3)
    request_data = BookingCreateRequest(
        master_id=test_master.id,
        client_id=uuid.uuid4(),
        start_at=future_time,
    )
    booking_resp, _ = await BookingService.create_booking(
        session=db_session,
        data=request_data,
        idempotency_key=str(uuid.uuid4()),
        correlation_id="test-create-corr",
    )

    mock_redis.delete.reset_mock()

    # Отменяем бронирование
    cancelled_resp = await BookingService.cancel_booking(
        session=db_session,
        booking_id=booking_resp.id,
        correlation_id="test-cancel-corr",
    )

    assert cancelled_resp.status == BookingStatus.CANCELLED

    # Проверяем создание события booking.cancelled в Outbox
    outbox_result = await db_session.execute(
        select(OutboxEvent).where(
            OutboxEvent.aggregate_id == booking_resp.id,
            OutboxEvent.event_type == "booking.cancelled",
        )
    )
    cancel_event = outbox_result.scalar_one_or_none()
    assert cancel_event is not None
    assert cancel_event.status == OutboxStatus.PENDING
    assert cancel_event.payload["correlation_id"] == "test-cancel-corr"

    # Проверяем повторную инвалидацию кэша (чтобы слот снова стал доступен)
    mock_redis.delete.assert_awaited_once()
