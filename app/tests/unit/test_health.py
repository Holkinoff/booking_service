from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.main import app


@pytest.mark.asyncio
async def test_healthz(client: AsyncClient):
    """Liveness probe возвращает 200 ok."""
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_readyz_all_healthy(client: AsyncClient):
    """Readiness probe возвращает 200 ready, если БД, Redis и RabbitMQ доступны."""
    with patch("app.api.health.check_rabbitmq_health", new_callable=AsyncMock) as mock_rabbit:
        mock_rabbit.return_value = True

        response = await client.get("/readyz")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ready"
        assert data["database"] is True
        assert data["redis"] is True
        assert data["rabbitmq"] is True


@pytest.mark.asyncio
async def test_readyz_database_down(client: AsyncClient):
    """Readiness probe возвращает 503, если БД недоступна."""
    mock_failing_session = AsyncMock(spec=AsyncSession)
    mock_failing_session.execute.side_effect = Exception("DB connection timeout")

    app.dependency_overrides[get_db] = lambda: mock_failing_session
    try:
        with patch("app.api.health.check_rabbitmq_health", new_callable=AsyncMock) as mock_rabbit:
            mock_rabbit.return_value = True

            response = await client.get("/readyz")
            assert response.status_code == 503
            data = response.json()
            assert data["detail"]["status"] == "unavailable"
            assert data["detail"]["database"] is False
            assert data["detail"]["redis"] is True
            assert data["detail"]["rabbitmq"] is True
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_readyz_redis_down(client: AsyncClient):
    """Readiness probe возвращает 503, если Redis недоступен."""
    failing_redis = AsyncMock()
    failing_redis.ping.side_effect = Exception("Redis unreachable")

    with patch("app.api.health.get_redis_client", return_value=failing_redis), \
         patch("app.api.health.check_rabbitmq_health", new_callable=AsyncMock) as mock_rabbit:
        mock_rabbit.return_value = True

        response = await client.get("/readyz")
        assert response.status_code == 503
        data = response.json()
        assert data["detail"]["status"] == "unavailable"
        assert data["detail"]["database"] is True
        assert data["detail"]["redis"] is False
        assert data["detail"]["rabbitmq"] is True


@pytest.mark.asyncio
async def test_readyz_rabbitmq_down(client: AsyncClient):
    """Readiness probe возвращает 503, если RabbitMQ недоступен."""
    with patch("app.api.health.check_rabbitmq_health", new_callable=AsyncMock) as mock_rabbit:
        mock_rabbit.return_value = False

        response = await client.get("/readyz")
        assert response.status_code == 503
        data = response.json()
        assert data["detail"]["status"] == "unavailable"
        assert data["detail"]["database"] is True
        assert data["detail"]["redis"] is True
        assert data["detail"]["rabbitmq"] is False


@pytest.mark.asyncio
async def test_readyz_all_services_down(client: AsyncClient):
    """Readiness probe возвращает 503, если все сервисы (БД, Redis, RabbitMQ) недоступны."""
    mock_failing_session = AsyncMock(spec=AsyncSession)
    mock_failing_session.execute.side_effect = Exception("DB error")

    failing_redis = AsyncMock()
    failing_redis.ping.side_effect = Exception("Redis error")

    app.dependency_overrides[get_db] = lambda: mock_failing_session
    try:
        with patch("app.api.health.get_redis_client", return_value=failing_redis), \
             patch("app.api.health.check_rabbitmq_health", new_callable=AsyncMock) as mock_rabbit:
            mock_rabbit.return_value = False

            response = await client.get("/readyz")
            assert response.status_code == 503
            data = response.json()
            assert data["detail"]["status"] == "unavailable"
            assert data["detail"]["database"] is False
            assert data["detail"]["redis"] is False
            assert data["detail"]["rabbitmq"] is False
    finally:
        app.dependency_overrides.pop(get_db, None)
