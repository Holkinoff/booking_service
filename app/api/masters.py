import uuid
from datetime import date

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas import MasterSlotsResponse
from app.services.slot_service import SlotService

router = APIRouter(prefix="/masters", tags=["Masters"])


@router.get(
    "/{master_id}/slots",
    response_model=MasterSlotsResponse,
    summary="Свободные слоты мастера на день",
)
async def get_master_slots(
    master_id: uuid.UUID,
    date: date,
    session: AsyncSession = Depends(get_db),
) -> MasterSlotsResponse:
    """
    Возвращает 30-минутные слоты мастера с 09:00 до 18:00.
    Результат кэшируется в Redis и инвалидируется при создании/отмене записей.
    """
    return await SlotService.get_master_slots(session, master_id, date)
