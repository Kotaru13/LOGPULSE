"""Тесты очереди, worker и статистики (на FakeRedis)."""
from logpulse import store, worker
from logpulse.parser import parse_lines
from tests.fake_redis import FakeRedis

L1 = "2026-10-20T12:34:56Z INFO auth 192.0.2.10 POST /api/login 200 43ms rid=ab12cd34"
L2 = "2026-10-20T12:34:58Z ERROR orders 198.51.100.8 GET /api/orders 503 742ms rid=ee44cc22"
L3 = "2026-10-20T12:35:01Z WARN payment 203.0.113.7 POST /api/pay 429 301ms rid=ff44aa33"
L4 = "2026-10-20T12:35:05Z ERROR payment 203.0.113.7 POST /api/pay 502 1900ms rid=11223344"


def ingest(r, *lines):
    batch = parse_lines(list(lines))
    assert not batch.errors
    store.enqueue(r, batch.valid)


def test_enqueue_adds_to_stream_and_counts_accepted(fake_redis):
    ingest(fake_redis, L1, L2)
    assert fake_redis.xlen(store.STREAM) == 2
    stats = store.read_stats(fake_redis)
    assert stats["accepted_total"] == 2
    assert stats["processed_total"] == 0 and stats["in_queue"] == 2


def test_counters_accumulate_after_worker_processing(fake_redis):
    ingest(fake_redis, L1, L2, L3, L4)
    assert worker.drain_once(fake_redis, "w1") == 4
    s = store.read_stats(fake_redis)
    assert s["processed_total"] == 4 and s["in_queue"] == 0
    assert s["by_level"] == {"INFO": 1, "ERROR": 2, "WARN": 1}
    assert s["by_service"] == {"auth": 1, "orders": 1, "payment": 2}
    assert s["by_status"] == {"200": 1, "503": 1, "429": 1, "502": 1}


def test_top_slow_sorted_desc_and_limited_to_five(fake_redis):
    ingest(fake_redis, L1, L2, L3, L4)
    worker.drain_once(fake_redis, "w1")
    top = store.read_stats(fake_redis)["top_slow"]
    assert top == [{"rid": "11223344", "duration_ms": 1900},
                   {"rid": "ee44cc22", "duration_ms": 742}]  # 43 и 301 мс — не медленные
    lines = [f"2026-10-20T12:36:00Z INFO auth 192.0.2.10 GET /api/me 200 {500 + i}ms rid={i:08x}"
             for i in range(8)]
    ingest(fake_redis, *lines)
    worker.drain_once(fake_redis, "w1")
    top = store.read_stats(fake_redis)["top_slow"]
    assert len(top) == 5 and top[0]["duration_ms"] == 1900
    assert [t["duration_ms"] for t in top] == sorted((t["duration_ms"] for t in top), reverse=True)


def test_message_acked_only_after_successful_processing(fake_redis):
    ingest(fake_redis, L1)
    store.ensure_group(fake_redis)
    assert worker.drain_once(fake_redis, "w1") == 1
    assert fake_redis.pending_count(store.STREAM, store.GROUP) == 0


def test_message_not_acked_when_redis_fails_mid_processing():
    class BrokenOnWrite(FakeRedis):
        def pipeline(self, transaction=True):
            from redis.exceptions import ConnectionError
            raise ConnectionError("обрыв связи")

    r = BrokenOnWrite()
    # публикуем событие напрямую в stream, минуя сломанный pipeline
    r.xadd(store.STREAM, parse_lines([L1]).valid[0])
    store.ensure_group(r)
    resp = r.xreadgroup(store.GROUP, "w1", {store.STREAM: ">"})
    try:
        worker.handle_messages(r, resp[0][1])
        raised = False
    except Exception:  # RedisError
        raised = True
    assert raised
    assert r.pending_count(store.STREAM, store.GROUP) == 1  # осталось без XACK


def test_redelivery_does_not_duplicate_statistics(fake_redis):
    ingest(fake_redis, L2)
    store.ensure_group(fake_redis)
    resp = fake_redis.xreadgroup(store.GROUP, "w1", {store.STREAM: ">"})
    msg_id, fields = resp[0][1][0]
    store.apply_event(fake_redis, store.event_from_fields(fields))  # «сбой» до XACK
    assert fake_redis.pending_count(store.STREAM, store.GROUP) == 1
    assert worker.process_pending(fake_redis, "w1") == 1  # повторная доставка
    s = store.read_stats(fake_redis)
    assert s["processed_total"] == 1 and s["by_status"] == {"503": 1}
    assert fake_redis.pending_count(store.STREAM, store.GROUP) == 0


def test_corrupt_message_is_acked_and_skipped(fake_redis):
    fake_redis.xadd(store.STREAM, {"foo": "bar"})
    assert worker.drain_once(fake_redis, "w1") == 1
    assert store.read_stats(fake_redis)["processed_total"] == 0
    assert fake_redis.pending_count(store.STREAM, store.GROUP) == 0


def test_ensure_group_is_idempotent(fake_redis):
    store.ensure_group(fake_redis)
    store.ensure_group(fake_redis)  # BUSYGROUP не должен приводить к ошибке
    assert (store.STREAM, store.GROUP) in fake_redis.groups
