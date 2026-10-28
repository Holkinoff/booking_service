import contextvars
import logging
import sys
from typing import Any

import structlog

# Контекстная переменная для сквозного correlation_id / request_id
correlation_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id",
    default="",
)


def add_correlation_id(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Добавляет correlation_id из контекста выполнения в каждую запись лога."""
    corr_id = correlation_id_ctx.get()
    if corr_id:
        event_dict["correlation_id"] = corr_id
    return event_dict


def setup_logging() -> None:
    """Настройка структурного JSON-логирования с помощью structlog."""
    # Отключаем дефолтные хендлеры uvicorn/standard logging, перенаправляя в structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            add_correlation_id,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


logger = structlog.get_logger()
