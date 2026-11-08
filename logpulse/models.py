"""Pydantic-модели запросов и ответов API."""
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

LogLine = Annotated[str, Field(min_length=1, max_length=500)]


class IngestRequest(BaseModel):
    """Пакет строк журнала: 1–100 строк, длина каждой 1–500 символов."""

    lines: list[LogLine] = Field(
        min_length=1, max_length=100, description="Строки журнала в формате LogPulse"
    )

    model_config = ConfigDict(json_schema_extra={"example": {"lines": [
        "2026-10-20T12:34:56Z INFO auth 192.0.2.10 POST /api/login 200 43ms rid=ab12cd34",
        "2026-10-20T12:34:58Z ERROR orders 198.51.100.8 GET /api/orders 503 742ms rid=ee44cc22",
    ]}})


class IngestError(BaseModel):
    line_no: int = Field(description="Номер строки в пакете (с 1)")
    reason: str
    line: str = Field(description="Исходная строка (секреты замаскированы, до 200 символов)")


class IngestResponse(BaseModel):
    accepted: int = Field(ge=0, description="Принято в очередь (не равно «обработано»)")
    rejected: int = Field(ge=0)
    errors: list[IngestError]


class SlowRequest(BaseModel):
    rid: str
    duration_ms: int


class StatsResponse(BaseModel):
    accepted_total: int = Field(description="Принято в очередь API")
    processed_total: int = Field(description="Обработано worker")
    in_queue: int = Field(description="accepted_total - processed_total")
    slow_threshold_ms: int
    by_level: dict[str, int]
    by_service: dict[str, int]
    by_status: dict[str, int]
    top_slow: list[SlowRequest]


class EventOut(BaseModel):
    id: str = Field(description="ID записи Redis Stream")
    timestamp: str
    level: str
    service: str
    ip: str
    method: str
    path: str
    status: int
    duration_ms: int
    rid: str


class EventsResponse(BaseModel):
    count: int
    events: list[EventOut]


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    redis: Literal["up"] = "up"


class ErrorDetail(BaseModel):
    loc: list[str]
    msg: str
    type: str


class ErrorBody(BaseModel):
    code: str
    message: str
    details: list[ErrorDetail] | None = None


class ErrorResponse(BaseModel):
    """Единый формат ошибок для всех кодов 4xx/5xx."""

    error: ErrorBody
