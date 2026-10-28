import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.logging import correlation_id_ctx
from app.schemas import BookingCreateRequest, BookingResponse
from app.services import BookingService

router = APIRouter(prefix="/bookings", tags=["Bookings"])


@router.post(
    "",
    response_model=BookingResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Создать запись на приём",
)
async def create_booking(
    data: BookingCreateRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key", description="Обязательный ключ идемпотентности"),
    session: AsyncSession = Depends(get_db),
) -> JSONResponse:
    """
    Создание бронирования:
    - Защита от дублей на уровне БД (частичный уникальный индекс).
    - Гарантия идемпотентности по Idempotency-Key.
    - Ограничение: нельзя записаться в прошлое или ближе чем за 60 минут.
    - Фиксация события в Transactional Outbox.
    """
    if not idempotency_key.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Заголовок Idempotency-Key не может быть пустым.",
        )

    corr_id = correlation_id_ctx.get() or str(uuid.uuid4())
    booking, status_code = await BookingService.create_booking(
        session=session,
        data=data,
        idempotency_key=idempotency_key,
        correlation_id=corr_id,
    )
    return JSONResponse(status_code=status_code, content=booking.model_dump(mode="json"))


@router.get(
    "/{booking_id}",
    response_model=BookingResponse,
    summary="Получить информацию о бронировании",
)
async def get_booking(
    booking_id: uuid.UUID,
    session: AsyncSession = Depends(get_db),
) -> BookingResponse:
    """Получение бронирования по его ID."""
    return await BookingService.get_booking(session, booking_id)


@router.post(
    "/{booking_id}/cancel",
    response_model=BookingResponse,
    summary="Отменить запись на приём",
)
async def cancel_booking(
    booking_id: uuid.UUID,
    session: AsyncSession = Depends(get_db),
) -> BookingResponse:
    """
    Отмена записи:
    - Переводит статус в CANCELLED.
    - Записывает событие booking.cancelled в outbox.
    - Инвалидирует кэш слотов, делая слот снова свободным.
    """
    corr_id = correlation_id_ctx.get() or str(uuid.uuid4())
    return await BookingService.cancel_booking(session, booking_id, correlation_id=corr_id)
