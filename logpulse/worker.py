"""Фоновый обработчик: XREADGROUP -> обновление статистики -> XACK.

Сообщение подтверждается (XACK) только после успешной обработки. Ошибки связи
с Redis не завершают процесс: worker ждёт (экспоненциальная пауза), заново
создаёт consumer group и сначала дообрабатывает неподтверждённые сообщения.
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import threading

from redis.exceptions import RedisError

from .config import get_settings, make_redis
from .store import GROUP, STREAM, apply_event, ensure_group, event_from_fields

log = logging.getLogger("logpulse.worker")
BATCH = 100


def handle_messages(r, messages) -> int:
    """Обрабатывает сообщения; XACK — только после успешного apply_event.
    RedisError пробрасывается наружу (сообщение остаётся в PEL)."""
    acked = 0
    for msg_id, fields in messages:
        try:
            event = event_from_fields(fields)
        except (KeyError, ValueError) as exc:
            # «ядовитое» сообщение: повторная доставка не поможет — подтверждаем
            log.warning("пропущено повреждённое сообщение %s: %r", msg_id, exc)
        else:
            apply_event(r, event)
        r.xack(STREAM, GROUP, msg_id)
        acked += 1
    return acked


def process_pending(r, consumer: str) -> int:
    """Дообрабатывает сообщения, доставленные этому consumer, но не подтверждённые."""
    total = 0
    while True:
        resp = r.xreadgroup(GROUP, consumer, {STREAM: "0"}, count=BATCH)
        messages = [m for _stream, msgs in (resp or []) for m in msgs]
        if not messages:
            return total
        total += handle_messages(r, messages)


def drain_once(r, consumer: str, count: int = BATCH) -> int:
    """Один неблокирующий проход по новым сообщениям (удобно для тестов)."""
    ensure_group(r)
    resp = r.xreadgroup(GROUP, consumer, {STREAM: ">"}, count=count)
    messages = [m for _stream, msgs in (resp or []) for m in msgs]
    return handle_messages(r, messages) if messages else 0


def run(r, consumer: str, stop: threading.Event, block_ms: int = 2000) -> None:
    need_setup, delay = True, 1.0
    while not stop.is_set():
        try:
            if need_setup:
                ensure_group(r)
                recovered = process_pending(r, consumer)
                if recovered:
                    log.info("дообработано %d неподтверждённых сообщений", recovered)
                need_setup = False
            resp = r.xreadgroup(GROUP, consumer, {STREAM: ">"},
                                count=BATCH, block=block_ms)
            for _stream, messages in resp or []:
                handle_messages(r, messages)
            delay = 1.0
        except (RedisError, OSError) as exc:
            log.error("ошибка связи с Redis (%s); повтор через %.0f с",
                      exc.__class__.__name__, delay)
            need_setup = True
            stop.wait(delay)
            delay = min(delay * 2, 30.0)


def main() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    consumer = os.getenv("WORKER_NAME", socket.gethostname())
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    log.info("worker %s запущен (redis=%s:%s)", consumer,
             settings.redis_host, settings.redis_port)
    run(make_redis(settings), consumer, stop)
    log.info("worker остановлен")


if __name__ == "__main__":
    main()
