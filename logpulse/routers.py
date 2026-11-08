from fastapi import APIRouter, Depends, Query

from . import store
from .deps import get_redis
from .models import (ErrorResponse, EventsResponse, HealthResponse, IngestError,
                     IngestRequest, IngestResponse, StatsResponse)
from .parser import parse_lines

ERRORS = {422: {"model": ErrorResponse}, 503: {"model": ErrorResponse}}

router = APIRouter()


@router.get("/health", response_model=HealthResponse, tags=["system"],
            summary="Доступность приложения и Redis", responses={503: ERRORS[503]})
def health(r=Depends(get_redis)):
    r.ping()  # при ошибке связи глобальный обработчик вернёт 503
    return HealthResponse()


@router.post("/ingest", response_model=IngestResponse, tags=["logs"],
             summary="Принять пакет строк журнала", responses=ERRORS)
def ingest(payload: IngestRequest, r=Depends(get_redis)):
    batch = parse_lines(payload.lines)
    store.enqueue(r, batch.valid)  # некорректные строки в очередь не попадают
    return IngestResponse(accepted=len(batch.valid), rejected=len(batch.errors),
                          errors=[IngestError(**e) for e in batch.errors])


@router.get("/stats", response_model=StatsResponse, tags=["logs"],
            summary="Агрегированная статистика из Redis", responses={503: ERRORS[503]})
def stats(r=Depends(get_redis)):
    return store.read_stats(r)


@router.get("/events", response_model=EventsResponse, tags=["logs"],
            summary="Последние записи Redis Stream", responses=ERRORS)
def events(limit: int = Query(10, ge=1, le=50, description="Сколько записей вернуть (1–50)"),
           r=Depends(get_redis)):
    items = store.last_events(r, limit)
    return EventsResponse(count=len(items), events=items)
