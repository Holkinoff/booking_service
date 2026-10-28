import uuid
from datetime import time
from typing import AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.core.database import get_db
from app.main import app
from app.models import Master

test_engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)


@pytest.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Изолированная сессия БД для каждого теста:
    Использует NullPool для устранения конфликта event loop в pytest-asyncio,
    и create_savepoint для чистого отката изменений после каждого теста.
    """
    async with test_engine.connect() as connection:
        trans = await connection.begin()
        session = AsyncSession(
            bind=connection,
            join_transaction_mode="create_savepoint",
            expire_on_commit=False,
        )
        try:
            yield session
        finally:
            await session.close()
            await trans.rollback()



@pytest.fixture
def mock_redis() -> AsyncMock:
    """Мок для Redis клиента, чтобы юнит-тесты не обращались к реальному Redis."""
    fake_redis = AsyncMock()
    fake_redis.get.return_value = None  # по умолчанию промах кэша
    fake_redis.set.return_value = True
    fake_redis.delete.return_value = True
    fake_redis.ping.return_value = True

    with patch("app.core.redis.get_redis_client", return_value=fake_redis), \
         patch("app.api.health.get_redis_client", return_value=fake_redis), \
         patch("app.services.slot_service.get_redis_client", return_value=fake_redis):
        yield fake_redis


@pytest.fixture
async def client(db_session: AsyncSession, mock_redis: AsyncMock) -> AsyncGenerator[AsyncClient, None]:
    """Тестовый HTTP-клиент FastAPI с подмененной сессией БД и моком Redis."""
    app.dependency_overrides[get_db] = lambda: db_session

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.fixture
async def test_master(db_session: AsyncSession) -> Master:
    """Создание тестового мастера в БД (откатывается после теста)."""
    master = Master(
        id=uuid.uuid4(),
        name="Иван Тестовый",
        work_start=time(9, 0),
        work_end=time(18, 0),
        slot_duration_minutes=30,
        is_active=True,
    )
    db_session.add(master)
    await db_session.commit()
    await db_session.refresh(master)
    return master
