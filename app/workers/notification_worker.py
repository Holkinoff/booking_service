import asyncio
import signal
import uuid
from datetime import UTC, datetime
from typing import Protocol

from aio_pika.abc import AbstractIncomingMessage
from redis.asyncio import Redis

from app.core.config import settings
from app.core.logging import correlation_id_ctx, logger, setup_logging
from app.core.rabbitmq import (
    close_rabbitmq_connection,
    get_rabbitmq_channel,
    setup_rabbitmq_topology,
)
from app.core.redis import close_redis_client, get_redis_client
from app.schemas import OutboxEventMessage


class NotificationSender(Protocol):
    """Интерфейс канала отправки уведомлений."""
    async def send(self, event: OutboxEventMessage) -> None: ...


class FakeHttpNotificationSender:
    """
    Фейковый канал отправки уведомлений клиенту.
    Формирует текст сообщения и фиксирует отправку в структурированном JSON-логе.
    Для проверки сбоев, ретраев и DLQ поддерживает опциональный флаг simulate_failure.
    """

    def __init__(self, simulate_failure: bool = False) -> None:
        self.simulate_failure = simulate_failure

    async def send(self, event: OutboxEventMessage) -> None:
        if self.simulate_failure:
            raise RuntimeError("Fake channel simulated dispatch failure")

        payload = event.payload
        message_text = (
            f"Уважаемый клиент! Напоминаем о вашей записи {payload.booking_id} на {payload.start_at}."
            if event.event_type == "booking.reminder"
            else f"Статус вашей записи {payload.booking_id} изменен на: {payload.status}."
        )

        logger.info(
            "notification_sent_to_client",
            event_id=str(event.event_id),
            event_type=event.event_type,
            client_id=str(payload.client_id),
            booking_id=str(payload.booking_id),
            channel="fake_http",
            message=message_text,
            correlation_id=event.correlation_id,
        )


class NotificationWorker:
    """
    Воркер-консьюмер сообщений из брокера RabbitMQ:
    1. Сквозное логирование в JSON по correlation_id.
    2. Дедупликация через Redis SETNX (at-least-once защита от повторных уведомлений).
    3. Rate Limiting через Redis INCR (не более 3 уведомлений в час на client_id).
    4. Отправка через канал уведомлений с ретраями с задержкой.
    5. Dead Letter Queue (DLQ) при исчерпании попыток.
    """

    def __init__(
        self,
        sender: NotificationSender | None = None,
        redis_client: Redis | None = None,
    ) -> None:
        self.sender = sender or FakeHttpNotificationSender()
        self._redis = redis_client

    async def _get_redis(self) -> Redis:
        if self._redis is None:
            self._redis = await get_redis_client()
        return self._redis

    async def check_deduplication(self, event_id: uuid.UUID) -> bool:
        """
        Проверка дедупликации:
        Возвращает True, если событие уже обрабатывалось ранее.
        Использует атомарный SETNX в Redis (TTL 7 дней).
        """
        redis = await self._get_redis()
        dedup_key = f"notification:dedup:{event_id}"
        is_new = await redis.set(dedup_key, "1", ex=7 * 86400, nx=True)
        return not bool(is_new)

    async def check_rate_limit(self, client_id: uuid.UUID) -> bool:
        """
        Проверка лимита: не более N уведомлений в час одному клиенту.
        Использует нативный атомарный счётчик INCR + EXPIRE в Redis на часовой бакет.
        """
        redis = await self._get_redis()
        current_hour = datetime.now(UTC).strftime("%Y%m%d%H")
        rate_key = f"notification:ratelimit:{client_id}:{current_hour}"

        count = await redis.incr(rate_key)
        if count == 1:
            await redis.expire(rate_key, 3600)

        return count <= settings.NOTIFICATION_HOURLY_LIMIT

    async def process_incoming_message(
        self,
        message: AbstractIncomingMessage,
    ) -> None:
        """Полный цикл обработки входящего сообщения из RabbitMQ."""
        # 1. Извлечение корреляционного ID и установка в structlog
        raw_corr_id = (
            (message.headers.get("correlation_id") if message.headers else None)
            or message.correlation_id
            or str(uuid.uuid4())
        )
        token = correlation_id_ctx.set(raw_corr_id)

        try:
            # 2. Десериализация и валидация через Pydantic
            body_text = message.body.decode("utf-8")
            event = OutboxEventMessage.model_validate_json(body_text)

            logger.info(
                "notification_event_received",
                event_id=str(event.event_id),
                event_type=event.event_type,
                booking_id=str(event.payload.booking_id),
                client_id=str(event.payload.client_id),
            )

            # 3. Дедупликация (at-least-once)
            if await self.check_deduplication(event.event_id):
                logger.info(
                    "notification_duplicate_skipped",
                    event_id=str(event.event_id),
                )
                await message.ack()
                return

            # 4. Проверка Rate Limit (счётчик в Redis: не более 3 в час)
            is_allowed = await self.check_rate_limit(event.payload.client_id)
            if not is_allowed:
                logger.warning(
                    "notification_rate_limited",
                    client_id=str(event.payload.client_id),
                    event_id=str(event.event_id),
                    hourly_limit=settings.NOTIFICATION_HOURLY_LIMIT,
                )
                await message.ack()
                return

            # 5. Отправка с ретраями с задержкой и сбросом в DLQ
            for attempt in range(1, settings.NOTIFICATION_MAX_RETRIES + 1):
                try:
                    await self.sender.send(event)
                    await message.ack()
                    logger.info(
                        "notification_processed_successfully",
                        event_id=str(event.event_id),
                        client_id=str(event.payload.client_id),
                        attempt=attempt,
                    )
                    return
                except Exception as exc:  # noqa: BLE001
                    if attempt < settings.NOTIFICATION_MAX_RETRIES:
                        logger.warning(
                            "notification_send_failed_retrying",
                            event_id=str(event.event_id),
                            attempt=attempt,
                            max_retries=settings.NOTIFICATION_MAX_RETRIES,
                            error=str(exc),
                        )
                        await asyncio.sleep(
                            settings.NOTIFICATION_RETRY_DELAY_SECONDS * attempt
                        )
                    else:
                        logger.error(
                            "notification_max_retries_exceeded_moving_to_dlq",
                            event_id=str(event.event_id),
                            retries=attempt,
                            max_retries=settings.NOTIFICATION_MAX_RETRIES,
                            error=str(exc),
                        )
                        # Отклоняем без requeue -> RabbitMQ маршрутизирует в DLQ
                        await message.reject(requeue=False)

        except Exception as exc:  # noqa: BLE001
            logger.error("notification_unhandled_processing_error", error=str(exc))
            await message.reject(requeue=False)
        finally:
            correlation_id_ctx.reset(token)

    async def start(
        self,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        """Запуск цикла прослушивания очереди уведомлений."""
        logger.info("notification_worker_starting")

        # Инициализация топологии RabbitMQ
        await setup_rabbitmq_topology()

        ch = await get_rabbitmq_channel()
        await ch.set_qos(prefetch_count=10)

        queue = await ch.get_queue(settings.RABBITMQ_NOTIFICATIONS_QUEUE)
        logger.info(
            "notification_worker_listening",
            queue=settings.RABBITMQ_NOTIFICATIONS_QUEUE,
        )

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                await self.process_incoming_message(message)
                if stop_event and stop_event.is_set():
                    break

        logger.info("notification_worker_stopped")


async def main() -> None:
    """Точка входа для запуска воркера как отдельного процесса."""
    setup_logging()
    logger.info("starting_notification_worker_process")

    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("shutdown_signal_received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    worker = NotificationWorker()
    try:
        await worker.start(stop_event=stop_event)
    finally:
        await close_redis_client()
        await close_rabbitmq_connection()


if __name__ == "__main__":
    asyncio.run(main())
