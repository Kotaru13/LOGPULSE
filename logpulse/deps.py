"""Зависимости FastAPI. get_redis можно переопределить через
app.dependency_overrides[get_redis] (так делают тесты)."""
from fastapi import Request


def get_redis(request: Request):
    return request.app.state.redis
