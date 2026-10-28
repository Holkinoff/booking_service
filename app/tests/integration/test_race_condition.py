import asyncio
import uuid
from datetime import UTC, datetime, time, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, func, select

from app.core.database import async_session_maker
from app.main import app
from app.models import Booking, BookingStatus, Master, OutboxEvent


@pytest.mark.asyncio
async def test_race_condition_20_concurrent_bookings():
    """
    Интеграционный тест на состояние гонки:
    20 одновременных запросов от разных клиентов на один и тот же слот.
    Проверяет защиту на уровне частичного уникального индекса PostgreSQL:
    - Ровно 1 запрос успешен (HTTP 201 Created).
    - Ровно 19 запросов отклонены (HTTP 409 Conflict).
    - В таблице bookings создана строго 1 подтвержденная запись.
    - В таблице outbox_events зафиксировано строго 1 событие booking.created.
    """
    # 1. Создаем уникального мастера для теста в БД
    master_id = uuid.uuid4()
    async with async_session_maker() as session:
        master = Master(
            id=master_id,
            name="Мастер для теста гонки",
            work_start=time(9, 0),
            work_end=time(18, 0),
            slot_duration_minutes=30,
            is_active=True,
        )
        session.add(master)
        await session.commit()

    # 2. Слот на завтра в 11:00 UTC (гарантированно > 60 мин от текущего времени)
    now_utc = datetime.now(UTC)
    target_slot = (now_utc + timedelta(days=1)).replace(
        hour=11, minute=0, second=0, microsecond=0
    )

    try:
        # 3. Функция отправки одного запроса
        async def send_booking_request(client_id: uuid.UUID) -> int:
            payload = {
                "master_id": str(master_id),
                "client_id": str(client_id),
                "start_at": target_slot.isoformat(),
            }
            headers = {
                "Idempotency-Key": f"race-{uuid.uuid4()}",
                "X-Request-ID": f"req-{uuid.uuid4()}",
            }
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.post("/bookings", json=payload, headers=headers)
                return resp.status_code

        # 4. Запускаем 20 параллельных корутин через asyncio.gather
        tasks = [send_booking_request(uuid.uuid4()) for _ in range(20)]
        results = await asyncio.gather(*tasks)

        # 5. Проверяем распределение статус-кодов
        status_201_count = results.count(201)
        status_409_count = results.count(409)

        assert status_201_count == 1, (
            f"Ожидался ровно 1 успешный ответ (201), получено {status_201_count}. "
            f"Все коды ответов: {results}"
        )
        assert status_409_count == 19, (
            f"Ожидалось ровно 19 конфликтов (409), получено {status_409_count}. "
            f"Все коды ответов: {results}"
        )

        # 6. Проверяем фактические записи в PostgreSQL
        async with async_session_maker() as session:
            # В таблице bookings должна быть строго 1 запись
            booking_count = await session.scalar(
                select(func.count())
                .select_from(Booking)
                .where(
                    Booking.master_id == master_id,
                    Booking.start_at == target_slot,
                    Booking.status == BookingStatus.CONFIRMED,
                )
            )
            assert booking_count == 1

            # В таблице outbox_events должно быть строго 1 событие
            outbox_count = await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(
                    OutboxEvent.event_type == "booking.created",
                    OutboxEvent.payload["master_id"].as_string() == str(master_id),
                )
            )
            assert outbox_count == 1

    finally:
        # 7. Очистка: удаляем тестового мастера и его события
        async with async_session_maker() as session:
            await session.execute(delete(Master).where(Master.id == master_id))
            await session.execute(
                delete(OutboxEvent).where(
                    OutboxEvent.payload["master_id"].as_string() == str(master_id)
                )
            )
            await session.commit()
