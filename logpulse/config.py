"""Настройки из переменных окружения (внутри Compose Redis доступен по имени redis)."""
from __future__ import annotations

import os
from dataclasses import dataclass

import redis


@dataclass(frozen=True)
class Settings:
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0
    redis_timeout: float = 5.0
    log_level: str = "INFO"


def get_settings() -> Settings:
    return Settings(
        redis_host=os.getenv("REDIS_HOST", "redis"),
        redis_port=int(os.getenv("REDIS_PORT", "6379")),
        redis_db=int(os.getenv("REDIS_DB", "0")),
        redis_timeout=float(os.getenv("REDIS_TIMEOUT", "5")),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )


def make_redis(settings: Settings | None = None) -> redis.Redis:
    """Создаёт клиент. Соединение устанавливается лениво — при первой команде,
    поэтому недоступный Redis не мешает запуску приложения."""
    s = settings or get_settings()
    return redis.Redis(
        host=s.redis_host,
        port=s.redis_port,
        db=s.redis_db,
        decode_responses=True,
        socket_timeout=s.redis_timeout,
        socket_connect_timeout=s.redis_timeout,
        health_check_interval=30,
    )
