import json
import uuid
from datetime import UTC, date, datetime, time, timedelta

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import logger
from app.core.redis import get_redis_client
from app.models import Booking, BookingStatus, Master
from app.schemas import MasterSlotsResponse, SlotResponse


class SlotService:
    """Сервис для работы с расписанием мастеров и кэшированием слотов в Redis."""

    @staticmethod
    def _slot_cache_key(master_id: uuid.UUID, slot_date: date) -> str:
        return f"slots:{master_id}:{slot_date.isoformat()}"

    @classmethod
    async def invalidate_slots_cache(cls, master_id: uuid.UUID, slot_date: date) -> None:
        """Инвалидация кэша свободных слотов в Redis при создании или отмене записи."""
        try:
            redis = await get_redis_client()
            key = cls._slot_cache_key(master_id, slot_date)
            await redis.delete(key)
            logger.info("slots_cache_invalidated", master_id=str(master_id), date=str(slot_date))
        except Exception as e:
            logger.warning("redis_cache_invalidation_failed", error=str(e))

    @classmethod
    async def get_master_slots(
        cls,
        session: AsyncSession,
        master_id: uuid.UUID,
        slot_date: date,
    ) -> MasterSlotsResponse:
        """
        Получение слотов мастера на день.
        Проверяет кэш в Redis. При промахе рассчитывает слоты из БД и кэширует.
        """
        redis = await get_redis_client()
        cache_key = cls._slot_cache_key(master_id, slot_date)

        # 1. Проверяем кэш в Redis
        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                logger.info("slots_cache_hit", master_id=str(master_id), date=str(slot_date))
                slots_dicts = json.loads(cached_data)
                return MasterSlotsResponse(
                    master_id=master_id,
                    date=slot_date.isoformat(),
                    slots=[SlotResponse(**s) for s in slots_dicts],
                )
        except Exception as e:
            logger.warning("redis_cache_read_failed", error=str(e))

        # 2. Проверяем мастера в БД
        master_result = await session.execute(
            select(Master).where(Master.id == master_id, Master.is_active.is_(True))
        )
        master = master_result.scalar_one_or_none()
        if not master:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Мастер не найден или не активен",
            )

        # 3. Выбираем занятые бронирования на эту дату
        start_of_day = datetime.combine(slot_date, time.min, tzinfo=UTC)
        end_of_day = datetime.combine(slot_date, time.max, tzinfo=UTC)

        bookings_result = await session.execute(
            select(Booking).where(
                Booking.master_id == master_id,
                Booking.status == BookingStatus.CONFIRMED,
                Booking.start_at >= start_of_day,
                Booking.start_at <= end_of_day,
            )
        )
        active_bookings = bookings_result.scalars().all()
        booked_start_times = {b.start_at for b in active_bookings}

        # 4. Генерируем 30-минутную сетку рабочего дня мастера
        slots: list[SlotResponse] = []
        current_time_dt = datetime.combine(slot_date, master.work_start, tzinfo=UTC)
        work_end_dt = datetime.combine(slot_date, master.work_end, tzinfo=UTC)
        slot_duration = timedelta(minutes=master.slot_duration_minutes)

        while current_time_dt + slot_duration <= work_end_dt:
            slot_end_dt = current_time_dt + slot_duration
            is_available = current_time_dt not in booked_start_times

            slots.append(
                SlotResponse(
                    start_at=current_time_dt,
                    end_at=slot_end_dt,
                    is_available=is_available,
                )
            )
            current_time_dt = slot_end_dt

        # 5. Сохраняем в кэш Redis (TTL 1 час)
        try:
            serialized_slots = [
                {
                    "start_at": s.start_at.isoformat(),
                    "end_at": s.end_at.isoformat(),
                    "is_available": s.is_available,
                }
                for s in slots
            ]
            await redis.set(cache_key, json.dumps(serialized_slots), ex=3600)
            logger.info("slots_cached_in_redis", master_id=str(master_id), date=str(slot_date))
        except Exception as e:
            logger.warning("redis_cache_write_failed", error=str(e))

        return MasterSlotsResponse(
            master_id=master_id,
            date=slot_date.isoformat(),
            slots=slots,
        )
