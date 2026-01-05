from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from src.services.dashboard_service import DashboardService

router = APIRouter(prefix="/api/v1/dashboard", tags=["Dashboard"])
dashboard_service = DashboardService()

@router.get("/metrics", summary="Métricas Dashboard", description="Retorna métricas para dashboard")
async def get_dashboard_metrics(
    start_date: Optional[str] = Query(None, description="Data inicial (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data final (YYYY-MM-DD)")
):
    """Retorna métricas agregadas para dashboard.
    
    Se start_date e end_date forem fornecidos, retorna métricas do período.
    Caso contrário, retorna métricas de hoje e últimos 7 dias.
    """
    try:
        metrics = dashboard_service.get_dashboard_metrics(start_date, end_date)
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/metrics/{date}", summary="Métricas por Data", description="Retorna métricas de uma data específica")
async def get_metrics_by_date(date: str):
    """Retorna métricas de uma data específica (formato: YYYY-MM-DD)."""
    try:
        metrics = dashboard_service.get_metrics_by_date(date)
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))