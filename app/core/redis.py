
import redis.asyncio as aioredis
from redis.asyncio import Redis

from app.core.config import settings

# Глобальный клиент Redis
redis_client: Redis | None = None


async def get_redis_client() -> Redis:
    """Возвращает инициализированный асинхронный клиент Redis."""
    global redis_client
    if redis_client is None:
        redis_client = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
        )
    return redis_client


async def close_redis_client() -> None:
    """Корректное закрытие соединений с Redis."""
    global redis_client
    if redis_client is not None:
        await redis_client.aclose()
        redis_client = None
