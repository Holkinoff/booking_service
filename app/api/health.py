from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.logging import logger
from app.core.redis import get_redis_client
from app.schemas import HealthResponse

router = APIRouter(tags=["Health"])


@router.get("/healthz", response_model=dict[str, str])
async def healthz() -> dict[str, str]:
    """Liveness probe: процесс запущен и отвечает."""
    return {"status": "ok"}


@router.get("/readyz", response_model=HealthResponse)
async def readyz(session: AsyncSession = Depends(get_db)) -> HealthResponse:
    """Readiness probe: проверка доступности PostgreSQL и Redis."""
    db_ok = False
    redis_ok = False

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

    all_ok = db_ok and redis_ok
    if not all_ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "unavailable", "database": db_ok, "redis": redis_ok},
        )

    return HealthResponse(status="ready", database=db_ok, redis=redis_ok)
