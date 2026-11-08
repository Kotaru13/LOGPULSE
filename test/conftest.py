import pytest

from tests.fake_redis import FakeRedis


@pytest.fixture
def fake_redis():
    """Чистая in-memory «база» на каждый тест — рабочие данные не затрагиваются."""
    return FakeRedis()


@pytest.fixture
def app(fake_redis):
    from logpulse.deps import get_redis
    from logpulse.main import create_app

    application = create_app()
    application.dependency_overrides[get_redis] = lambda: fake_redis
    return application


@pytest.fixture
def client(app):
    from fastapi.testclient import TestClient

    return TestClient(app)  # без "with": lifespan (реальный Redis) не запускается


@pytest.fixture
def down_client(app):
    from fastapi.testclient import TestClient

    from logpulse.deps import get_redis
    from tests.fake_redis import DownRedis

    app.dependency_overrides[get_redis] = lambda: DownRedis()
    return TestClient(app)
