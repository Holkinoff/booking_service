import asyncio
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import async_session_maker
from app.core.logging import logger
from app.models import Booking, BookingStatus, OutboxEvent, OutboxStatus


class ReminderService:
    """
    Планировщик напоминаний о визите за 24 часа.
    Находит предстоящие подтвержденные бронирования,
    помечает их reminder_sent=True и генерирует событие в Transactional Outbox.
    """

    @classmethod
    async def fetch_pending_reminders(
        cls,
        session: AsyncSession,
        hours_before: int = settings.REMINDER_HOURS_BEFORE,
        limit: int = settings.REMINDER_BATCH_SIZE,
    ) -> list[Booking]:
        """
        Выборка подтвержденных бронирований, до которых осталось менее hours_before часов,
        с блокировкой FOR UPDATE SKIP LOCKED для безопасного масштабирования.
        """
        now = datetime.now(UTC)
        threshold = now + timedelta(hours=hours_before)

        stmt = (
            select(Booking)
            .where(
                Booking.status == BookingStatus.CONFIRMED,
                Booking.reminder_sent.is_(False),
                Booking.start_at <= threshold,
                Booking.start_at > now,  # Не отправляем напоминания на прошедшие визиты
            )
            .order_by(Booking.start_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    @classmethod
    async def process_reminders(
        cls,
        session: AsyncSession,
        hours_before: int = settings.REMINDER_HOURS_BEFORE,
        limit: int = settings.REMINDER_BATCH_SIZE,
    ) -> int:
        """
        Обработка порции напоминаний:
        Для каждой брони ставится флаг reminder_sent = True
        и создается событие в outbox_events для гарантированной доставки брокером.
        """
        bookings = await cls.fetch_pending_reminders(
            session=session,
            hours_before=hours_before,
            limit=limit,
        )
        if not bookings:
            return 0

        scheduled_count = 0
        now = datetime.now(UTC)

        for booking in bookings:
            booking.reminder_sent = True
            correlation_id = f"reminder-{booking.id}-{int(now.timestamp())}"

            outbox_event = OutboxEvent(
                id=uuid.uuid4(),
                aggregate_type="booking",
                aggregate_id=booking.id,
                event_type="booking.reminder",
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
                created_at=now,
            )
            session.add(outbox_event)
            scheduled_count += 1

            logger.info(
                "booking_reminder_scheduled",
                booking_id=str(booking.id),
                client_id=str(booking.client_id),
                start_at=booking.start_at.isoformat(),
                correlation_id=correlation_id,
            )

        await session.commit()
        return scheduled_count

    @classmethod
    async def run_reminder_scheduler(
        cls,
        poll_interval: float = settings.REMINDER_POLL_INTERVAL_SECONDS,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        """
        Фоновый цикл планировщика напоминаний:
        Периодически опрашивает БД на наличие бронирований, требующих напоминания.
        """
        logger.info("reminder_scheduler_loop_started", poll_interval=poll_interval)

        while not (stop_event and stop_event.is_set()):
            scheduled_count = 0
            try:
                async with async_session_maker() as session:
                    scheduled_count = await cls.process_reminders(session=session)
            except Exception as exc:  # noqa: BLE001
                logger.error("reminder_scheduler_cycle_error", error=str(exc))

            # Если были найдены напоминания, проверяем следующую пачку без задержки
            if scheduled_count > 0:
                continue

            # Засыпаем на poll_interval, либо мгновенно прерываем паузу при сигнале остановки (graceful shutdown)
            if stop_event:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
                    break  # Получен сигнал остановки -> немедленно выходим из цикла
                except TimeoutError:
                    # Штатное поведение: время паузы истекло, сигнала остановки не было.
                    # Переходим к следующему циклу опроса БД.
                    continue
            else:
                await asyncio.sleep(poll_interval)

        logger.info("reminder_scheduler_loop_stopped")
