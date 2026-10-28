import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Booking, BookingStatus, Master, OutboxEvent
from app.services.reminder_service import ReminderService


@pytest.fixture
def make_booking():
    def _create(
        master_id: uuid.UUID,
        client_id: uuid.UUID,
        start_at: datetime,
        status: BookingStatus = BookingStatus.CONFIRMED,
        reminder_sent: bool = False,
    ) -> Booking:
        now = datetime.now(UTC)
        return Booking(
            id=uuid.uuid4(),
            master_id=master_id,
            client_id=client_id,
            start_at=start_at,
            end_at=start_at + timedelta(minutes=30),
            status=status,
            reminder_sent=reminder_sent,
            created_at=now,
            updated_at=now,
        )
    return _create


@pytest.mark.asyncio
async def test_reminder_service_filters_bookings_within_24h(
    db_session: AsyncSession,
    test_master: Master,
    make_booking,
):
    """Шедулер выбирает только те бронирования, до которых осталось <= 24 часов и reminder_sent=False."""
    now = datetime.now(UTC)
    client_id = uuid.uuid4()

    # 1. Подходит: через 12 часов, reminder_sent=False, CONFIRMED
    b_eligible = make_booking(
        master_id=test_master.id,
        client_id=client_id,
        start_at=now + timedelta(hours=12),
        reminder_sent=False,
    )
    # 2. Не подходит: через 36 часов (> 24ч)
    b_future = make_booking(
        master_id=test_master.id,
        client_id=client_id,
        start_at=now + timedelta(hours=36),
        reminder_sent=False,
    )
    # 3. Не подходит: уже отправлено (reminder_sent=True)
    b_already_sent = make_booking(
        master_id=test_master.id,
        client_id=client_id,
        start_at=now + timedelta(hours=10),
        reminder_sent=True,
    )
    # 4. Не подходит: отменена (CANCELLED)
    b_cancelled = make_booking(
        master_id=test_master.id,
        client_id=client_id,
        start_at=now + timedelta(hours=8),
        status=BookingStatus.CANCELLED,
        reminder_sent=False,
    )
    # 5. Не подходит: в прошлом
    b_past = make_booking(
        master_id=test_master.id,
        client_id=client_id,
        start_at=now - timedelta(hours=2),
        reminder_sent=False,
    )

    db_session.add_all([b_eligible, b_future, b_already_sent, b_cancelled, b_past])
    await db_session.commit()

    candidates = await ReminderService.fetch_pending_reminders(
        session=db_session,
        hours_before=24,
    )

    assert len(candidates) == 1
    assert candidates[0].id == b_eligible.id


@pytest.mark.asyncio
async def test_process_reminders_creates_outbox_and_marks_flag(
    db_session: AsyncSession,
    test_master: Master,
    make_booking,
):
    """process_reminders устанавливает reminder_sent=True и генерирует событие в Outbox."""
    now = datetime.now(UTC)
    client_id = uuid.uuid4()

    b1 = make_booking(
        master_id=test_master.id,
        client_id=client_id,
        start_at=now + timedelta(hours=5),
        reminder_sent=False,
    )
    db_session.add(b1)
    await db_session.commit()

    processed_count = await ReminderService.process_reminders(
        session=db_session,
        hours_before=24,
    )

    assert processed_count == 1

    # Проверяем флаг reminder_sent
    await db_session.refresh(b1)
    assert b1.reminder_sent is True

    # Проверяем Outbox событие
    stmt = select(OutboxEvent).where(OutboxEvent.aggregate_id == b1.id)
    result = await db_session.execute(stmt)
    event = result.scalar_one()

    assert event.event_type == "booking.reminder"
    assert event.payload["booking_id"] == str(b1.id)
    assert event.payload["client_id"] == str(client_id)
