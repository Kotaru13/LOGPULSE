"""Работа со структурами Redis: очередь (Stream), статистика (Hash, Sorted Set).

Ключи:
    logpulse:events         Stream      очередь событий (XADD / XREADGROUP / XACK)
    logpulse:stats:level    Hash        счётчики по уровням
    logpulse:stats:service  Hash        счётчики по сервисам
    logpulse:stats:status   Hash        счётчики по HTTP-кодам
    logpulse:slow           Sorted Set  рейтинг медленных запросов (score = duration_ms)
    logpulse:stats:accepted String      сколько событий принято в очередь
    logpulse:stats:processed String     сколько событий обработано worker
    logpulse:seen           Set         rid уже учтённых событий (идемпотентность)
"""
from __future__ import annotations

from redis.exceptions import ResponseError

STREAM = "logpulse:events"
GROUP = "logpulse-workers"
LEVEL_KEY = "logpulse:stats:level"
SERVICE_KEY = "logpulse:stats:service"
STATUS_KEY = "logpulse:stats:status"
SLOW_KEY = "logpulse:slow"
ACCEPTED_KEY = "logpulse:stats:accepted"
PROCESSED_KEY = "logpulse:stats:processed"
SEEN_KEY = "logpulse:seen"

SLOW_THRESHOLD_MS = 500
TOP_SLOW = 5
EVENT_FIELDS = ("timestamp", "level", "service", "ip", "method", "path",
                "status", "duration_ms", "rid")


# ----------------------------------------------------------------- очередь --
def enqueue(r, events: list[dict]) -> int:
    """Публикует валидные события одним атомарным пакетом (XADD) и увеличивает
    счётчик «принято в очередь». Возвращает число добавленных событий."""
    if not events:
        return 0
    pipe = r.pipeline(transaction=True)
    for event in events:
        pipe.xadd(STREAM, {name: event[name] for name in EVENT_FIELDS})
    pipe.incrby(ACCEPTED_KEY, len(events))
    pipe.execute()
    return len(events)


def ensure_group(r) -> None:
    """Создаёт consumer group (и stream при необходимости). Повтор безопасен."""
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def event_from_fields(fields: dict) -> dict:
    """Строки из Redis -> типизированное событие. KeyError/ValueError при порче."""
    event = {name: fields[name] for name in EVENT_FIELDS}
    event["status"] = int(event["status"])
    event["duration_ms"] = int(event["duration_ms"])
    return event


# ------------------------------------------------------------- обработка ----
def apply_event(r, event: dict) -> bool:
    """Обновляет статистику. Идемпотентно по rid: повторная доставка того же
    сообщения не меняет счётчики. Все изменения — в одной транзакции MULTI/EXEC.
    Возвращает True, если событие учтено, False — если это дубликат."""
    rid = event["rid"]
    if r.sismember(SEEN_KEY, rid):
        return False
    pipe = r.pipeline(transaction=True)
    pipe.sadd(SEEN_KEY, rid)
    pipe.hincrby(LEVEL_KEY, event["level"], 1)
    pipe.hincrby(SERVICE_KEY, event["service"], 1)
    pipe.hincrby(STATUS_KEY, str(event["status"]), 1)
    pipe.incr(PROCESSED_KEY)
    if event["duration_ms"] >= SLOW_THRESHOLD_MS:
        pipe.zadd(SLOW_KEY, {rid: event["duration_ms"]})
    pipe.execute()
    return True


# ------------------------------------------------------------- чтение -------
def _to_int_map(raw: dict) -> dict[str, int]:
    return {k: int(v) for k, v in raw.items()}


def read_stats(r) -> dict:
    pipe = r.pipeline(transaction=False)
    pipe.get(ACCEPTED_KEY)
    pipe.get(PROCESSED_KEY)
    pipe.hgetall(LEVEL_KEY)
    pipe.hgetall(SERVICE_KEY)
    pipe.hgetall(STATUS_KEY)
    pipe.zrevrange(SLOW_KEY, 0, TOP_SLOW - 1, withscores=True)
    accepted, processed, levels, services, statuses, slow = pipe.execute()
    accepted, processed = int(accepted or 0), int(processed or 0)
    return {
        "accepted_total": accepted,
        "processed_total": processed,
        "in_queue": max(accepted - processed, 0),
        "slow_threshold_ms": SLOW_THRESHOLD_MS,
        "by_level": _to_int_map(levels),
        "by_service": _to_int_map(services),
        "by_status": _to_int_map(statuses),
        "top_slow": [{"rid": rid, "duration_ms": int(score)} for rid, score in slow],
    }


def last_events(r, limit: int) -> list[dict]:
    """Последние записи Stream (новые первыми)."""
    out = []
    for entry_id, fields in r.xrevrange(STREAM, count=limit):
        item = dict(fields)
        item["id"] = entry_id
        out.append(item)
    return out
