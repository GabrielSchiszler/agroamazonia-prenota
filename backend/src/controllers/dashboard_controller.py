import logging
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from src.services.dashboard_service import DashboardService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dashboard", tags=["Dashboard"])
service = DashboardService()

@router.get("/metrics", summary="Métricas Dashboard")
async def get_dashboard_metrics(
    start_date: Optional[str] = Query(None, description="Data inicial (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data final (YYYY-MM-DD)")
):
    """Retorna métricas agregadas. Se start_date e end_date forem fornecidos, retorna métricas do período."""
    logger.info("=" * 80)
    logger.info("[get_dashboard_metrics] Requisição recebida para obter métricas do dashboard")
    logger.info(f"[get_dashboard_metrics] start_date: {start_date} (tipo: {type(start_date)})")
    logger.info(f"[get_dashboard_metrics] end_date: {end_date} (tipo: {type(end_date)})")
    
    try:
        logger.info("[get_dashboard_metrics] Chamando service.get_dashboard_metrics...")
        result = service.get_dashboard_metrics(start_date, end_date)
        logger.info(f"[get_dashboard_metrics] Métricas obtidas com sucesso!")
        logger.info("=" * 80)
        return result
    except Exception as e:
        logger.error(f"[get_dashboard_metrics] Erro inesperado: {str(e)}")
        logger.error(f"[get_dashboard_metrics] Tipo do erro: {type(e).__name__}")
        logger.exception("[get_dashboard_metrics] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/metrics/{date}", summary="Métricas por Data")
async def get_metrics_by_date(date: str):
    """Retorna métricas de uma data específica (formato: YYYY-MM-DD)"""
    logger.info("=" * 80)
    logger.info("[get_metrics_by_date] Requisição recebida para obter métricas por data")
    logger.info(f"[get_metrics_by_date] date: {date} (tipo: {type(date)})")
    
    try:
        logger.info("[get_metrics_by_date] Chamando service.get_metrics_by_date...")
        result = service.get_metrics_by_date(date)
        logger.info(f"[get_metrics_by_date] Métricas obtidas com sucesso!")
        logger.info("=" * 80)
        return result
    except Exception as e:
        logger.error(f"[get_metrics_by_date] Erro inesperado: {str(e)}")
        logger.error(f"[get_metrics_by_date] Tipo do erro: {type(e).__name__}")
        logger.exception("[get_metrics_by_date] Traceback completo:")
        logger.info("=" * 80)
        raise HTTPException(status_code=500, detail=str(e))
