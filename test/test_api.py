"""Тесты HTTP API (TestClient + переопределение зависимости get_redis)."""
from logpulse import worker

GOOD = [
    "2026-10-20T12:34:56Z INFO auth 192.0.2.10 POST /api/login 200 43ms rid=ab12cd34",
    "2026-10-20T12:34:58Z ERROR orders 198.51.100.8 GET /api/orders 503 742ms rid=ee44cc22",
    "2026-10-20T12:35:01Z WARN payment 203.0.113.7 POST /api/pay 429 301ms rid=ff44aa33",
]
BAD = [
    "2026-10-20T12:35:02Z INFO auth 192.0.2.999 GET /api/me 200 12ms rid=aa11bb22",
    "2026-10-20T12:35:03Z INFO auth 192.0.2.12 GET /api/me 200 15ms rid=zz12cd3",
]


def test_ingest_mixed_batch_3_valid_2_invalid(client):
    resp = client.post("/ingest", json={"lines": [GOOD[0], BAD[0], GOOD[1], BAD[1], GOOD[2]]})
    assert resp.status_code == 200
    body = resp.json()
    assert body["accepted"] == 3 and body["rejected"] == 2
    assert [e["line_no"] for e in body["errors"]] == [2, 4]
    assert "IPv4" in body["errors"][0]["reason"] and "rid" in body["errors"][1]["reason"]


def test_invalid_lines_never_reach_the_queue(client, fake_redis):
    client.post("/ingest", json={"lines": BAD})
    assert fake_redis.xlen("logpulse:events") == 0


def test_ingest_wrong_schema_returns_422_unified_format(client):
    for payload in ({"lines": "строка"}, {"wrong": []}, {"lines": [1, 2]}):
        resp = client.post("/ingest", json=payload)
        assert resp.status_code == 422
        err = resp.json()["error"]
        assert err["code"] == "validation_error" and err["details"]


def test_ingest_limits_100_lines_and_500_chars(client):
    assert client.post("/ingest", json={"lines": ["x"] * 101}).status_code == 422
    assert client.post("/ingest", json={"lines": []}).status_code == 422
    assert client.post("/ingest", json={"lines": ["x" * 501]}).status_code == 422
    assert client.post("/ingest", json={"lines": [""]}).status_code == 422
    assert client.post("/ingest", json={"lines": ["x"] * 100}).status_code == 200


def test_stats_after_worker_processing(client, fake_redis):
    client.post("/ingest", json={"lines": GOOD})
    before = client.get("/stats").json()
    assert before["accepted_total"] == 3 and before["processed_total"] == 0
    assert before["in_queue"] == 3
    worker.drain_once(fake_redis, "test-worker")
    stats = client.get("/stats").json()
    assert stats["processed_total"] == 3 and stats["in_queue"] == 0
    assert stats["by_level"] == {"INFO": 1, "ERROR": 1, "WARN": 1}
    assert stats["by_status"] == {"200": 1, "503": 1, "429": 1}
    assert stats["top_slow"] == [{"rid": "ee44cc22", "duration_ms": 742}]


def test_health_ok_with_redis(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "redis": "up"}


def test_health_503_when_redis_down(down_client):
    resp = down_client.get("/health")
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "redis_unavailable"
    assert "Traceback" not in resp.text


def test_ingest_stats_events_503_when_redis_down(down_client):
    assert down_client.post("/ingest", json={"lines": GOOD}).status_code == 503
    assert down_client.get("/stats").status_code == 503
    assert down_client.get("/events").status_code == 503


def test_events_limit_bounds_and_order(client):
    client.post("/ingest", json={"lines": GOOD})
    data = client.get("/events", params={"limit": 2}).json()
    assert data["count"] == 2
    assert data["events"][0]["rid"] == "ff44aa33"  # новые первыми
    assert data["events"][0]["status"] == 429
    assert client.get("/events", params={"limit": 0}).status_code == 422
    assert client.get("/events", params={"limit": 51}).status_code == 422
    assert client.get("/events").json()["count"] == 3  # limit по умолчанию 10


def test_openapi_and_swagger_available(client):
    schema = client.get("/openapi.json").json()
    assert {"/health", "/ingest", "/stats", "/events"} <= set(schema["paths"])
    assert client.get("/docs").status_code == 200
