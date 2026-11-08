"""Единый формат ошибок. Клиенту не отдаются трассировки и внутренние детали."""
import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError
from starlette.exceptions import HTTPException as StarletteHTTPException

log = logging.getLogger("logpulse.api")


def error_response(status: int, code: str, message: str, details=None) -> JSONResponse:
    body = {"code": code, "message": message}
    if details is not None:
        body["details"] = details
    return JSONResponse(status_code=status, content={"error": body})


async def on_validation_error(request: Request, exc: RequestValidationError):
    details = [
        {"loc": [str(p) for p in e.get("loc", ())], "msg": str(e.get("msg", "")),
         "type": str(e.get("type", ""))}
        for e in exc.errors()
    ]
    return error_response(422, "validation_error", "Некорректные данные запроса", details)


async def on_redis_error(request: Request, exc: RedisError):
    log.error("Redis недоступен или вернул ошибку: %s", exc.__class__.__name__)
    return error_response(503, "redis_unavailable", "Redis временно недоступен")


async def on_http_error(request: Request, exc: StarletteHTTPException):
    return error_response(exc.status_code, "http_error", str(exc.detail))


async def on_unhandled(request: Request, exc: Exception):
    log.exception("необработанная ошибка")  # трассировка — только в журнал сервера
    return error_response(500, "internal_error", "Внутренняя ошибка сервера")


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(RequestValidationError, on_validation_error)
    app.add_exception_handler(RedisError, on_redis_error)
    app.add_exception_handler(StarletteHTTPException, on_http_error)
    app.add_exception_handler(Exception, on_unhandled)
