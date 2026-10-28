import uuid
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import logger
from app.models import (
    Booking,
    BookingStatus,
    IdempotencyKey,
    Master,
    OutboxEvent,
    OutboxStatus,
)
from app.schemas import BookingCreateRequest, BookingResponse
from app.services.slot_service import SlotService


class BookingService:
    """Сервис управления бронированиями, идемпотентностью и событиями outbox."""

    @classmethod
    async def create_booking(
        cls,
        session: AsyncSession,
        data: BookingCreateRequest,
        idempotency_key: str,
        correlation_id: str,
    ) -> tuple[BookingResponse, int]:
        """
        Создание бронирования с проверкой идемпотентности, защитой от гонок на уровне БД
        и записью события в Transactional Outbox.
        """
        # 1. Проверка Idempotency-Key
        idemp_result = await session.execute(
            select(IdempotencyKey).where(IdempotencyKey.key == idempotency_key)
        )
        existing_idemp = idemp_result.scalar_one_or_none()
        if existing_idemp:
            logger.info("idempotency_hit", key=idempotency_key)
            return BookingResponse(**existing_idemp.response_body), existing_idemp.response_code

        # 2. Проверка: Слот в прошлом или ближе чем за 60 минут
        now_utc = datetime.now(UTC)
        min_allowed_time = now_utc + timedelta(minutes=settings.BOOKING_MIN_MINUTES_BEFORE_SLOT)

        # Приводим к UTC если таймзона не указана
        slot_start_utc = data.start_at if data.start_at.tzinfo else data.start_at.replace(tzinfo=UTC)

        if slot_start_utc < min_allowed_time:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Нельзя записаться на время в прошлом или ближе чем за {settings.BOOKING_MIN_MINUTES_BEFORE_SLOT} минут от текущего времени.",
            )

        # 3. Проверка мастера
        master_result = await session.execute(
            select(Master).where(Master.id == data.master_id, Master.is_active.is_(True))
        )
        master = master_result.scalar_one_or_none()
        if not master:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Мастер не найден или не активен",
            )

        # 4. Расчёт времени окончания слота
        slot_end_utc = slot_start_utc + timedelta(minutes=master.slot_duration_minutes)

        # Создаем запись бронирования
        booking = Booking(
            id=uuid.uuid4(),
            master_id=data.master_id,
            client_id=data.client_id,
            start_at=slot_start_utc,
            end_at=slot_end_utc,
            status=BookingStatus.CONFIRMED,
            reminder_sent=False,
            created_at=now_utc,
            updated_at=now_utc,
        )
        session.add(booking)

        # 5. Формируем событие Transactional Outbox
        outbox_event = OutboxEvent(
            id=uuid.uuid4(),
            aggregate_type="booking",
            aggregate_id=booking.id,
            event_type="booking.created",
            payload={
                "booking_id": str(booking.id),
                "master_id": str(booking.master_id),
                "client_id": str(booking.client_id),
                "start_at": booking.start_at.isoformat(),
                "end_at": booking.end_at.isoformat(),
                "status": booking.status.value,
                "correlation_id": correlation_id,
            },
            status=OutboxStatus.PENDING,
        )
        session.add(outbox_event)

        # 6. Сохраняем результат в IdempotencyKey
        response_model = BookingResponse.model_validate(booking)
        idemp_record = IdempotencyKey(
            key=idempotency_key,
            client_id=data.client_id,
            request_path="/bookings",
            response_code=status.HTTP_201_CREATED,
            response_body=response_model.model_dump(mode="json"),
        )
        session.add(idemp_record)

        # 7. Фиксация транзакции с перехватом конфликта уникальности (гонки на один слот)
        try:
            await session.commit()
            await session.refresh(booking)
        except IntegrityError as exc:
            await session.rollback()
            # Проверяем, был ли нарушен уникальный частичный индекс слота
            if "uq_booking_master_active_slot" in str(exc) or "unique" in str(exc).lower():
                logger.warning(
                    "slot_race_conflict_prevented",
                    master_id=str(data.master_id),
                    start_at=slot_start_utc.isoformat(),
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Выбранный слот уже занят другим клиентом",
                )
            raise

        # 8. Инвалидируем кэш слотов для этой даты через SlotService
        await SlotService.invalidate_slots_cache(master.id, slot_start_utc.date())

        logger.info(
            "booking_created",
            booking_id=str(booking.id),
            master_id=str(master.id),
            client_id=str(data.client_id),
        )
        return BookingResponse.model_validate(booking), status.HTTP_201_CREATED

    @classmethod
    async def get_booking(cls, session: AsyncSession, booking_id: uuid.UUID) -> BookingResponse:
        """Получение бронирования по ID."""
        result = await session.execute(select(Booking).where(Booking.id == booking_id))
        booking = result.scalar_one_or_none()
        if not booking:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Бронирование не найдено",
            )
        return BookingResponse.model_validate(booking)

    @classmethod
    async def cancel_booking(
        cls,
        session: AsyncSession,
        booking_id: uuid.UUID,
        correlation_id: str,
    ) -> BookingResponse:
        """Отмена бронирования с записью события в Outbox и инвалидацией кэша."""
        result = await session.execute(select(Booking).where(Booking.id == booking_id))
        booking = result.scalar_one_or_none()
        if not booking:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Бронирование не найдено",
            )

        if booking.status == BookingStatus.CANCELLED:
            return BookingResponse.model_validate(booking)

        booking.status = BookingStatus.CANCELLED

        # Outbox событие отмены
        outbox_event = OutboxEvent(
            id=uuid.uuid4(),
            aggregate_type="booking",
            aggregate_id=booking.id,
            event_type="booking.cancelled",
            payload={
                "booking_id": str(booking.id),
                "master_id": str(booking.master_id),
                "client_id": str(booking.client_id),
                "start_at": booking.start_at.isoformat(),
                "end_at": booking.end_at.isoformat(),
                "status": booking.status.value,
                "correlation_id": correlation_id,
            },
            status=OutboxStatus.PENDING,
        )
        session.add(outbox_event)

        await session.commit()
        await session.refresh(booking)

        # Инвалидируем кэш слотов, чтобы отмененный слот снова стал доступен
        await SlotService.invalidate_slots_cache(booking.master_id, booking.start_at.date())

        logger.info("booking_cancelled", booking_id=str(booking.id))
        return BookingResponse.model_validate(booking)
