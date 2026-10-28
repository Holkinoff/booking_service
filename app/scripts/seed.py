import asyncio
import uuid
from datetime import time

from sqlalchemy import select

from app.core.database import async_session_maker
from app.models import Master

# Фиксированные UUID для предсказуемости в тестах и запросах
DEFAULT_MASTERS = [
    {
        "id": uuid.UUID("11111111-1111-1111-1111-111111111111"),
        "name": "Алексей Смирнов",
        "work_start": time(9, 0),
        "work_end": time(18, 0),
        "slot_duration_minutes": 30,
        "is_active": True,
    },
    {
        "id": uuid.UUID("22222222-2222-2222-2222-222222222222"),
        "name": "Елена Васильева",
        "work_start": time(9, 0),
        "work_end": time(18, 0),
        "slot_duration_minutes": 30,
        "is_active": True,
    },
    {
        "id": uuid.UUID("33333333-3333-3333-3333-333333333333"),
        "name": "Дмитрий Иванов",
        "work_start": time(9, 0),
        "work_end": time(18, 0),
        "slot_duration_minutes": 30,
        "is_active": True,
    },
]


async def seed_masters() -> None:
    """Идемпотентное заполнение таблицы мастеров начальными данными."""
    print("Начало сидирования мастеров...")

    async with async_session_maker() as session:
        for master_data in DEFAULT_MASTERS:
            # Проверяем, существует ли мастер с таким ID
            result = await session.execute(
                select(Master).where(Master.id == master_data["id"])
            )
            existing_master = result.scalar_one_or_none()

            if existing_master is None:
                master = Master(**master_data)
                session.add(master)
                print(f" [+] Добавлен мастер: {master_data['name']} (id: {master_data['id']})")
            else:
                print(f" [~] Мастер уже существует: {master_data['name']} (id: {master_data['id']})")

        await session.commit()

    print("Сидирование успешно завершено!")


if __name__ == "__main__":
    asyncio.run(seed_masters())
