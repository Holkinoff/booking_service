import enum
import uuid
from datetime import datetime, time
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Time,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Базовый класс для всех моделей SQLAlchemy."""


class BookingStatus(str, enum.Enum):
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"


class OutboxStatus(str, enum.Enum):
    PENDING = "PENDING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


class Master(Base):
    """Мастер / специалист."""
    __tablename__ = "masters"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    work_start: Mapped[time] = mapped_column(Time, default=time(9, 0), nullable=False)
    work_end: Mapped[time] = mapped_column(Time, default=time(18, 0), nullable=False)
    slot_duration_minutes: Mapped[int] = mapped_column(Integer, default=30, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    bookings: Mapped[list["Booking"]] = relationship(
        "Booking",
        back_populates="master",
        cascade="all, delete-orphan",
    )


class Booking(Base):
    """Запись клиента к мастеру на определенный слот."""
    __tablename__ = "bookings"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    master_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("masters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    client_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    start_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )
    end_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    status: Mapped[BookingStatus] = mapped_column(
        String(32),
        default=BookingStatus.CONFIRMED,
        nullable=False,
        index=True,
    )
    reminder_sent: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default="false",
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    master: Mapped["Master"] = relationship("Master", back_populates="bookings")

    __table_args__ = (
        # Защита от одновременных записей на один слот на уровне БД:
        # Уникальность master_id + start_at действует ТОЛЬКО для активных (CONFIRMED) записей.
        # Если запись была отменена (CANCELLED), слот снова доступен для бронирования.
        Index(
            "uq_booking_master_active_slot",
            "master_id",
            "start_at",
            unique=True,
            postgresql_where=(status == BookingStatus.CONFIRMED),
        ),
        CheckConstraint("end_at > start_at", name="chk_booking_duration"),
    )


class IdempotencyKey(Base):
    """
    Таблица для сохранения результата запросов по Idempotency-Key.
    Предотвращает повторное выполнение бизнес-логики и создание дублирующих записей.
    """
    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    client_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
    )
    request_path: Mapped[str] = mapped_column(String(255), nullable=False)
    response_code: Mapped[int] = mapped_column(Integer, nullable=False)
    response_body: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class OutboxEvent(Base):
    """
    Паттерн Transactional Outbox.
    События сохраняются в одной ACID-транзакции с бронью, 
    гарантируя, что они не потеряются даже при временной недоступности RabbitMQ.
    """
    __tablename__ = "outbox_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    aggregate_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default="booking",
    )
    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    event_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True,
    )  # booking.created, booking.cancelled
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[OutboxStatus] = mapped_column(
        String(32),
        default=OutboxStatus.PENDING,
        nullable=False,
        index=True,
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        # Индекс для быстрого чтения необработанных событий воркером/паблишером
        Index(
            "idx_outbox_events_pending",
            "created_at",
            postgresql_where=(status == OutboxStatus.PENDING),
        ),
    )

