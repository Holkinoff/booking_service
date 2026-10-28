import json
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Booking, BookingStatus, Master
from app.services.slot_service import SlotService


@pytest.mark.asyncio
async def test_generate_slots_grid(db_session: AsyncSession, test_master: Master, mock_redis):
    """Проверка генерации 30-минутной сетки слотов с 09:00 до 18:00 (18 слотов)."""
    target_date = date(2026, 11, 10)
    response = await SlotService.get_master_slots(db_session, test_master.id, target_date)

    assert response.master_id == test_master.id
    assert response.date == "2026-11-10"
    assert len(response.slots) == 18

    # Проверяем первый слот: 09:00 - 09:30
    first_slot = response.slots[0]
    assert first_slot.start_at.hour == 9
    assert first_slot.start_at.minute == 0
    assert first_slot.end_at.hour == 9
    assert first_slot.end_at.minute == 30
    assert first_slot.is_available is True

    # Проверяем последний слот: 17:30 - 18:00
    last_slot = response.slots[-1]
    assert last_slot.start_at.hour == 17
    assert last_slot.start_at.minute == 30
    assert last_slot.end_at.hour == 18
    assert last_slot.end_at.minute == 0
    assert last_slot.is_available is True

    # Проверяем, что результат был сохранен в кэш Redis с TTL 3600
    mock_redis.set.assert_awaited_once()
    args, kwargs = mock_redis.set.call_args
    assert f"slots:{test_master.id}:2026-11-10" in args[0]
    assert kwargs.get("ex") == 3600


@pytest.mark.asyncio
async def test_slot_occupied_when_confirmed_booking_exists(
    db_session: AsyncSession,
    test_master: Master,
    mock_redis,
):
    """Слот должен быть помечен как занятый (is_available=False), если есть подтвержденная бронь."""
    target_date = date(2026, 11, 10)
    slot_time = datetime(2026, 11, 10, 11, 0, tzinfo=timezone.utc)

    # Создаем подтвержденную бронь на 11:00
    booking = Booking(
        id=uuid.uuid4(),
        master_id=test_master.id,
        client_id=uuid.uuid4(),
        start_at=slot_time,
        end_at=slot_time + timedelta(minutes=30),
        status=BookingStatus.CONFIRMED,
    )
    db_session.add(booking)
    await db_session.commit()

    response = await SlotService.get_master_slots(db_session, test_master.id, target_date)

    # Находим слот на 11:00
    slot_11 = next(s for s in response.slots if s.start_at == slot_time)
    assert slot_11.is_available is False

    # Остальные слоты должны быть свободны
    other_slots = [s for s in response.slots if s.start_at != slot_time]
    assert all(s.is_available is True for s in other_slots)


@pytest.mark.asyncio
async def test_slot_available_when_booking_cancelled(
    db_session: AsyncSession,
    test_master: Master,
    mock_redis,
):
    """Отмененная бронь (CANCELLED) не должна блокировать слот."""
    target_date = date(2026, 11, 10)
    slot_time = datetime(2026, 11, 10, 14, 0, tzinfo=timezone.utc)

    # Создаем отмененную бронь на 14:00
    booking = Booking(
        id=uuid.uuid4(),
        master_id=test_master.id,
        client_id=uuid.uuid4(),
        start_at=slot_time,
        end_at=slot_time + timedelta(minutes=30),
        status=BookingStatus.CANCELLED,
    )
    db_session.add(booking)
    await db_session.commit()

    response = await SlotService.get_master_slots(db_session, test_master.id, target_date)

    slot_14 = next(s for s in response.slots if s.start_at == slot_time)
    assert slot_14.is_available is True


@pytest.mark.asyncio
async def test_slots_returned_from_cache(db_session: AsyncSession, test_master: Master, mock_redis):
    """Если данные есть в Redis кэше, они отдаются напрямую без запроса к БД."""
    target_date = date(2026, 11, 10)
    cached_slots = [
        {
            "start_at": "2026-11-10T09:00:00Z",
            "end_at": "2026-11-10T09:30:00Z",
            "is_available": False,
        }
    ]
    mock_redis.get.return_value = json.dumps(cached_slots)

    response = await SlotService.get_master_slots(db_session, test_master.id, target_date)

    assert len(response.slots) == 1
    assert response.slots[0].is_available is False
    mock_redis.get.assert_awaited_once()


@pytest.mark.asyncio
async def test_invalidate_slots_cache(mock_redis):
    """Инвалидация кэша вызывает удаление соответствующего ключа в Redis."""
    master_id = uuid.uuid4()
    slot_date = date(2026, 11, 10)

    await SlotService.invalidate_slots_cache(master_id, slot_date)

    mock_redis.delete.assert_awaited_once_with(f"slots:{master_id}:2026-11-10")


@pytest.mark.asyncio
async def test_get_slots_inactive_master(db_session: AsyncSession, mock_redis):
    """Запрос слотов для неактивного или несуществующего мастера возвращает 404."""
    unknown_id = uuid.uuid4()
    with pytest.raises(HTTPException) as exc_info:
        await SlotService.get_master_slots(db_session, unknown_id, date(2026, 11, 10))

    assert exc_info.value.status_code == 404
