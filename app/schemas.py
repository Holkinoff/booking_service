import uuid
from datetime import datetime, time
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.models import BookingStatus


# --- Мастера и слоты ---

class MasterResponse(BaseModel):
    id: uuid.UUID
    name: str
    work_start: time
    work_end: time
    slot_duration_minutes: int
    is_active: bool

    model_config = ConfigDict(from_attributes=True)


class SlotResponse(BaseModel):
    start_at: datetime
    end_at: datetime
    is_available: bool


class MasterSlotsResponse(BaseModel):
    master_id: uuid.UUID
    date: str
    slots: list[SlotResponse]


# --- Бронирования ---

class BookingCreateRequest(BaseModel):
    master_id: uuid.UUID = Field(..., description="ID мастера")
    client_id: uuid.UUID = Field(..., description="ID клиента")
    start_at: datetime = Field(..., description="Время начала слота в формате ISO 8601 (UTC)")


class BookingResponse(BaseModel):
    id: uuid.UUID
    master_id: uuid.UUID
    client_id: uuid.UUID
    start_at: datetime
    end_at: datetime
    status: BookingStatus
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# --- События (Outbox / RabbitMQ) ---

class BookingEventPayload(BaseModel):
    """Данные события бронирования."""
    booking_id: uuid.UUID = Field(..., description="ID бронирования")
    master_id: uuid.UUID = Field(..., description="ID мастера")
    client_id: uuid.UUID = Field(..., description="ID клиента")
    start_at: datetime = Field(..., description="Время начала слота")
    end_at: datetime = Field(..., description="Время окончания слота")
    status: str = Field(..., description="Статус бронирования (CONFIRMED, CANCELLED)")
    correlation_id: str = Field(..., description="Сквозной Correlation ID для логирования")

    model_config = ConfigDict(from_attributes=True)


class OutboxEventMessage(BaseModel):
    """Схема сериализации/валидации события для передачи через RabbitMQ."""
    event_id: uuid.UUID = Field(..., description="Уникальный ID события (для дедупликации в воркере)")
    aggregate_type: str = Field(default="booking", description="Тип агрегата (booking)")
    aggregate_id: Optional[uuid.UUID] = Field(default=None, description="Идентификатор сущности/бронирования")
    event_type: str = Field(..., description="Тип события: booking.created | booking.cancelled")
    payload: BookingEventPayload = Field(..., description="Полезная нагрузка события")
    correlation_id: Optional[str] = Field(default=None, description="Сквозной идентификатор запроса/события для сквозного логирования")
    created_at: datetime = Field(..., description="Время генерации события в UTC")

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="after")
    def populate_defaults_from_payload(self) -> "OutboxEventMessage":
        if self.aggregate_id is None and self.payload is not None:
            self.aggregate_id = self.payload.booking_id
        if self.correlation_id is None and self.payload is not None:
            self.correlation_id = self.payload.correlation_id
        return self


# --- Здоровье сервиса ---

class HealthResponse(BaseModel):
    status: str
    database: bool
    redis: bool
    rabbitmq: bool
