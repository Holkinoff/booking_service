from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.logging import logger
from app.core.rabbitmq import check_rabbitmq_health
from app.core.redis import get_redis_client
from app.schemas import HealthResponse

router = APIRouter(tags=["Health"])


@router.get("/healthz", response_model=dict[str, str])
async def healthz() -> dict[str, str]:
    """Liveness probe: процесс запущен и отвечает."""
    return {"status": "ok"}


@router.get("/readyz", response_model=HealthResponse)
async def readyz(session: AsyncSession = Depends(get_db)) -> HealthResponse:
    """Readiness probe: проверка доступности PostgreSQL, Redis и RabbitMQ."""
    db_ok = False
    redis_ok = False
    rabbitmq_ok = False

    # Проверка БД
    try:
        await session.execute(text("SELECT 1"))
        db_ok = True
    except Exception as exc:
        logger.error("readiness_db_failed", error=str(exc))

    # Проверка Redis
    try:
        redis = await get_redis_client()
        await redis.ping()
        redis_ok = True
    except Exception as exc:
        logger.error("readiness_redis_failed", error=str(exc))

    # Проверка RabbitMQ
    try:
        rabbitmq_ok = await check_rabbitmq_health()
    except Exception as exc:
        logger.error("readiness_rabbitmq_failed", error=str(exc))

    all_ok = db_ok and redis_ok and rabbitmq_ok
    if not all_ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "status": "unavailable",
                "database": db_ok,
                "redis": redis_ok,
                "rabbitmq": rabbitmq_ok,
            },
        )

    return HealthResponse(
        status="ready",
        database=db_ok,
        redis=redis_ok,
        rabbitmq=rabbitmq_ok,
    )
