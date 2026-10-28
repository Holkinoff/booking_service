import asyncio
import time
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, Response

from app.api.router import api_router
from app.core.logging import correlation_id_ctx, logger, setup_logging
from app.core.rabbitmq import close_rabbitmq_connection
from app.core.redis import close_redis_client
from app.services.outbox_service import OutboxService


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Управление жизненным циклом приложения FastAPI."""
    setup_logging()
    logger.info("service_starting", service="booking-service")

    # Фоновый воркер пересылки событий из Outbox в брокер
    stop_event = asyncio.Event()
    outbox_task = asyncio.create_task(
        OutboxService.run_outbox_publisher(stop_event=stop_event)
    )

    yield

    logger.info("service_shutting_down", service="booking-service")
    # Сигнализируем воркеру об остановке и даём завершить текущую пачку
    stop_event.set()
    try:
        await asyncio.wait_for(outbox_task, timeout=5.0)
    except TimeoutError:
        logger.warning("outbox_publisher_shutdown_timeout", timeout_sec=5.0)
        outbox_task.cancel()

    await close_redis_client()
    await close_rabbitmq_connection()


app = FastAPI(
    title="Booking Service",
    description="Сервис записи клиентов на приём к мастеру с уведомлениями",
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next: Any) -> Response:  # type: ignore
    """
    Middleware для сквозного Correlation-ID:
    Извлекает или генерирует X-Request-ID, логирует запрос/ответ в формате JSON.
    """
    corr_id = request.headers.get("X-Request-ID") or request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    token = correlation_id_ctx.set(corr_id)

    start_time = time.perf_counter()
    logger.info(
        "http_request_started",
        method=request.method,
        path=request.url.path,
        query=str(request.query_params),
    )

    try:
        response: Response = await call_next(request)
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        response.headers["X-Request-ID"] = corr_id
        logger.info(
            "http_request_completed",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_ms,
        )
        return response
    except Exception as exc:
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        logger.error(
            "http_request_failed",
            method=request.method,
            path=request.url.path,
            error=str(exc),
            duration_ms=duration_ms,
        )
        raise
    finally:
        correlation_id_ctx.reset(token)


# Подключаем роуты API
app.include_router(api_router)
