"""Интеграционный тест с НАСТОЯЩИМ Redis. Запускается только если задан
TEST_REDIS_URL (например redis://redis:6379/15). Используется отдельная БД с
номером ≠ 0, которая очищается — рабочие данные (БД 0) не затрагиваются."""
import os

import pytest

URL = os.getenv("TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(not URL, reason="TEST_REDIS_URL не задан")


def test_real_redis_full_cycle():
    import redis

    from logpulse import store, worker
    from logpulse.parser import parse_lines

    r = redis.Redis.from_url(URL, decode_responses=True)
    assert r.connection_pool.connection_kwargs.get("db", 0) != 0, "нужна тестовая БД ≠ 0"
    r.flushdb()
    try:
        line = "2026-10-20T12:34:58Z ERROR orders 198.51.100.8 GET /api/orders 503 742ms rid=ee44cc22"
        store.enqueue(r, parse_lines([line]).valid)
        assert r.type(store.STREAM) == "stream"
        assert worker.drain_once(r, "it-worker") == 1
        assert r.type(store.LEVEL_KEY) == "hash" and r.type(store.SLOW_KEY) == "zset"
        assert store.read_stats(r)["top_slow"] == [{"rid": "ee44cc22", "duration_ms": 742}]
    finally:
        r.flushdb()
