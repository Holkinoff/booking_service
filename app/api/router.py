from fastapi import APIRouter

from app.api.bookings import router as bookings_router
from app.api.health import router as health_router
from app.api.masters import router as masters_router

api_router = APIRouter()

api_router.include_router(health_router)
api_router.include_router(masters_router)
api_router.include_router(bookings_router)
