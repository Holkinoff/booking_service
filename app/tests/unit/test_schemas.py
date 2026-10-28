import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.schemas import (
    BookingCreateRequest,
    BookingEventPayload,
    OutboxEventMessage,
)


def test_booking_create_request_valid():
    """Корректные типы успешно парсятся в Pydantic модель."""
    master_id = uuid.uuid4()
    client_id = uuid.uuid4()
    now_iso = datetime.now(UTC).isoformat()

    req = BookingCreateRequest(
        master_id=master_id,
        client_id=client_id,
        start_at=now_iso,  # строка ISO 8601 приводится к datetime
    )
    assert req.master_id == master_id
    assert req.client_id == client_id
    assert isinstance(req.start_at, datetime)


def test_booking_create_request_invalid_uuid():
    """Невалидный UUID вызывает ошибку валидации ValidationError."""
    with pytest.raises(ValidationError):
        BookingCreateRequest(
            master_id="not-a-valid-uuid",  # type: ignore
            client_id=uuid.uuid4(),
            start_at=datetime.now(UTC),
        )


def test_outbox_event_message_serialization():
    """События для очередей RabbitMQ корректно сериализуются в JSON."""
    event_id = uuid.uuid4()
    booking_id = uuid.uuid4()
    master_id = uuid.uuid4()
    client_id = uuid.uuid4()
    now = datetime.now(UTC)

    msg = OutboxEventMessage(
        event_id=event_id,
        event_type="booking.created",
        payload=BookingEventPayload(
            booking_id=booking_id,
            master_id=master_id,
            client_id=client_id,
            start_at=now,
            end_at=now,
            status="CONFIRMED",
            correlation_id="corr-123",
        ),
        created_at=now,
    )

    json_str = msg.model_dump_json()
    assert str(event_id) in json_str
    assert "booking.created" in json_str
    assert "corr-123" in json_str
