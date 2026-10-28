from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # PostgreSQL
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/booking_db"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # RabbitMQ
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBITMQ_EVENTS_EXCHANGE: str = "bookings.events"
    RABBITMQ_NOTIFICATIONS_QUEUE: str = "booking.notifications.queue"
    RABBITMQ_DLX_EXCHANGE: str = "bookings.dlx"
    RABBITMQ_NOTIFICATIONS_DLQ: str = "booking.notifications.dlq"
    RABBITMQ_DLQ_ROUTING_KEY: str = "booking.notifications.dead"

    # Бизнес-настройки
    BOOKING_MIN_MINUTES_BEFORE_SLOT: int = 60
    NOTIFICATION_HOURLY_LIMIT: int = 3

    # Outbox-пересыльщик
    OUTBOX_POLL_INTERVAL_SECONDS: float = 2.0
    OUTBOX_BATCH_SIZE: int = 50
    OUTBOX_MAX_RETRIES: int = 5

    # Напоминания (Scheduler)
    REMINDER_HOURS_BEFORE: int = 24
    REMINDER_POLL_INTERVAL_SECONDS: float = 60.0
    REMINDER_BATCH_SIZE: int = 100

    # Notification Worker & Retries
    NOTIFICATION_RETRY_DELAY_SECONDS: float = 2.0
    NOTIFICATION_MAX_RETRIES: int = 3

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
