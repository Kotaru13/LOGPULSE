import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .config import get_settings, make_redis
from .errors import register_error_handlers
from .routers import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(level=settings.log_level,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    app.state.redis = make_redis(settings)  # соединение ленивое: старт не падает без Redis
    try:
        yield
    finally:
        app.state.redis.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="LogPulse",
        version="1.0.0",
        description="Приём и статистика журналов сервисов auth / orders / payment "
                    "(FastAPI + Redis Streams).",
        lifespan=lifespan,
    )
    app.include_router(router)
    register_error_handlers(app)
    return app


app = create_app()
