import asyncio
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import async_session_maker
from app.core.logging import correlation_id_ctx, logger
from app.core.rabbitmq import publish_event, setup_rabbitmq_topology
from app.models import OutboxEvent, OutboxStatus
from app.schemas import BookingEventPayload, OutboxEventMessage


class OutboxService:
    """Сервис Transactional Outbox для гарантированной доставки событий в RabbitMQ."""

    @classmethod
    async def fetch_pending_events(
        cls,
        session: AsyncSession,
        limit: int = settings.OUTBOX_BATCH_SIZE,
    ) -> list[OutboxEvent]:
        """
        Выборка PENDING событий из БД с блокировкой FOR UPDATE SKIP LOCKED.
        Гарантирует, что при увелчиении числа воркеров
        одно и то же событие не будет выбрано двумя процессами одновременно.
        """
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.PENDING)
            .order_by(OutboxEvent.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    @classmethod
    async def process_event(
        cls,
        session: AsyncSession,
        event: OutboxEvent,
    ) -> bool:
        """
        Обработка единичного события:
        1. Валидация через Pydantic-схему.
        2. Отправка в RabbitMQ с correlation_id.
        3. Обновление статуса в БД: PROCESSED при успехе, FAILED при превышении ретраев.
        """
        corr_id = event.payload.get("correlation_id") or str(event.id)
        token = correlation_id_ctx.set(corr_id)

        try:
            payload = BookingEventPayload.model_validate(event.payload)
            message = OutboxEventMessage(
                event_id=event.id,
                aggregate_type=event.aggregate_type,
                aggregate_id=event.aggregate_id,
                event_type=event.event_type,
                payload=payload,
                correlation_id=corr_id,
                created_at=event.created_at,
            )

            # Публикуем событие в брокер
            await publish_event(event=message)

            # При успешной публикации меняем статус на PROCESSED
            event.status = OutboxStatus.PROCESSED
            event.processed_at = datetime.now(UTC)
            await session.commit()

            logger.info(
                "outbox_event_processed_successfully",
                event_id=str(event.id),
                event_type=event.event_type,
                aggregate_id=str(event.aggregate_id),
            )
            return True

        except Exception as exc:  # noqa: BLE001
            event.retry_count += 1

            if event.retry_count >= settings.OUTBOX_MAX_RETRIES:
                event.status = OutboxStatus.FAILED
                logger.error(
                    "outbox_event_failed_max_retries",
                    event_id=str(event.id),
                    retries=event.retry_count,
                    max_retries=settings.OUTBOX_MAX_RETRIES,
                    error=str(exc),
                )
            else:
                logger.warning(
                    "outbox_event_publish_failed_retrying",
                    event_id=str(event.id),
                    retries=event.retry_count,
                    max_retries=settings.OUTBOX_MAX_RETRIES,
                    error=str(exc),
                )

            await session.commit()
            return False

        finally:
            correlation_id_ctx.reset(token)

    @classmethod
    async def process_pending_events(
        cls,
        session: AsyncSession,
        batch_size: int = settings.OUTBOX_BATCH_SIZE,
    ) -> int:
        """
        Обработка порции (батча) необработанных событий из outbox_events.
        Возвращает количество успешно отправленных событий.
        """
        events = await cls.fetch_pending_events(session=session, limit=batch_size)
        if not events:
            return 0

        success_count = 0
        for event in events:
            is_success = await cls.process_event(session=session, event=event)
            if is_success:
                success_count += 1

        return success_count

    @classmethod
    async def run_outbox_publisher(
        cls,
        poll_interval: float = settings.OUTBOX_POLL_INTERVAL_SECONDS,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        """
        Фоновый цикл пересыльщика Outbox:
        Периодически опрашивает БД и отправляет PENDING события в RabbitMQ.
        Останавливается по сигналу stop_event.
        """
        logger.info("outbox_publisher_loop_started", poll_interval=poll_interval)

        # Обеспечиваем наличие топологии RabbitMQ перед отправкой
        try:
            await setup_rabbitmq_topology()
        except Exception as exc:  # noqa: BLE001
            logger.warning("outbox_publisher_topology_init_warning", error=str(exc))

        while not (stop_event and stop_event.is_set()):
            success_count = 0
            try:
                async with async_session_maker() as session:
                    success_count = await cls.process_pending_events(session=session)
            except Exception as exc:  # noqa: BLE001
                logger.error("outbox_publisher_cycle_error", error=str(exc))

            # Если события были обработаны, сразу переходим к следующей пачке без задержек
            if success_count > 0:
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

        logger.info("outbox_publisher_loop_stopped")
