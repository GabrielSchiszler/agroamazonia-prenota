from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from src.services.dashboard_service import DashboardService

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])
service = DashboardService()

@router.get("/metrics", summary="Métricas Dashboard")
async def get_dashboard_metrics(
    start_date: Optional[str] = Query(None, description="Data inicial (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data final (YYYY-MM-DD)")
):
    """Retorna métricas agregadas. Se start_date e end_date forem fornecidos, retorna métricas do período."""
    try:
        return service.get_dashboard_metrics(start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/metrics/{date}", summary="Métricas por Data")
async def get_metrics_by_date(date: str):
    """Retorna métricas de uma data específica (formato: YYYY-MM-DD)"""
    try:
        return service.get_metrics_by_date(date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
