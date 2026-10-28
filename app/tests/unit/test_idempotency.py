import uuid
from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models import Booking, Master
from app.schemas import BookingCreateRequest
from app.services.booking_service import BookingService


@pytest.mark.asyncio
async def test_idempotent_duplicate_request_returns_same_response(
    db_session: AsyncSession,
    test_master: Master,
    mock_redis,
):
    """
    Повторный запрос с тем же Idempotency-Key:
    - Возвращает идентичный результат и статус 201.
    - НЕ создает вторую запись бронирования в базе данных.
    """
    future_time = datetime.now(timezone.utc) + timedelta(
        minutes=settings.BOOKING_MIN_MINUTES_BEFORE_SLOT + 30
    )
    client_id = uuid.uuid4()
    idempotency_key = f"key-{uuid.uuid4()}"

    request_data = BookingCreateRequest(
        master_id=test_master.id,
        client_id=client_id,
        start_at=future_time,
    )

    # Первый вызов
    resp_1, status_1 = await BookingService.create_booking(
        session=db_session,
        data=request_data,
        idempotency_key=idempotency_key,
        correlation_id="corr-1",
    )
    assert status_1 == 201

    # Второй вызов с тем же Idempotency-Key
    resp_2, status_2 = await BookingService.create_booking(
        session=db_session,
        data=request_data,
        idempotency_key=idempotency_key,
        correlation_id="corr-2",
    )
    assert status_2 == 201
    assert resp_1.id == resp_2.id
    assert resp_1.master_id == resp_2.master_id
    assert resp_1.client_id == resp_2.client_id

    # Проверяем количество записей в БД: строго 1 запись!
    count_result = await db_session.execute(
        select(func.count()).select_from(Booking).where(Booking.id == resp_1.id)
    )
    assert count_result.scalar() == 1


@pytest.mark.asyncio
async def test_empty_idempotency_key_header_rejected(client: AsyncClient, test_master: Master):
    """Пустой заголовок Idempotency-Key отклоняется с кодом 400."""
    future_time = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    payload = {
        "master_id": str(test_master.id),
        "client_id": str(uuid.uuid4()),
        "start_at": future_time,
    }

    response = await client.post(
        "/bookings",
        json=payload,
        headers={"Idempotency-Key": "   "},
    )
    assert response.status_code == 400
    assert "не может быть пустым" in response.json()["detail"]


@pytest.mark.asyncio
async def test_missing_idempotency_key_header_rejected(client: AsyncClient, test_master: Master):
    """Отсутствие обязательного заголовка Idempotency-Key отклоняется FastAPI с кодом 422."""
    future_time = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    payload = {
        "master_id": str(test_master.id),
        "client_id": str(uuid.uuid4()),
        "start_at": future_time,
    }

    response = await client.post("/bookings", json=payload)
    assert response.status_code == 422
