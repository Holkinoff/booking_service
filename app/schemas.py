import uuid
from datetime import datetime, time
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

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
    booking_id: uuid.UUID
    master_id: uuid.UUID
    client_id: uuid.UUID
    start_at: datetime
    end_at: datetime
    status: str
    correlation_id: str


class OutboxEventMessage(BaseModel):
    event_id: uuid.UUID
    event_type: str  # "booking.created" | "booking.cancelled"
    payload: BookingEventPayload
    created_at: datetime


# --- Здоровье сервиса ---

class HealthResponse(BaseModel):
    status: str
    database: bool
    redis: bool
