"""Парсер строк журнала LogPulse.

Формат строки (все поля разделены одним пробелом)::

    2026-10-20T12:34:56Z INFO auth 192.0.2.10 POST /api/login 200 43ms rid=ab12cd34

Этапы разбора:
1. очистка строки (``clean_line``, регулярное выражение CONTROL_CHARS_RE);
2. полное сопоставление с ОДНОЙ основной маской LINE_RE (``re.fullmatch``);
3. семантическая проверка полей: datetime, ipaddress, диапазоны, словари
   допустимых значений, дополнительные регулярные выражения RID_RE и PATH_RE;
4. маскирование секретов в пути (SECRET_RE).
"""
from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass, field
from datetime import datetime

# --------------------------------------------------------------------------
# Основная маска с именованными группами.
# Группы намеренно «широкие»: точная причина отказа определяется на втором
# этапе и попадает в понятное сообщение об ошибке.
# --------------------------------------------------------------------------
LINE_RE = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) "
    r"(?P<level>[A-Za-z]+) "
    r"(?P<service>[A-Za-z]+) "
    r"(?P<ip>[0-9.]+) "
    r"(?P<method>[A-Za-z]+) "
    r"(?P<path>\S+) "
    r"(?P<status>\d+) "
    r"(?P<duration_ms>-?\d+)ms "
    r"rid=(?P<rid>\S+)"
)

# --------------------------------------------------------------------------
# Дополнительные регулярные выражения (каждое проверено отдельным тестом).
# --------------------------------------------------------------------------
#: 1. rid — ровно 8 шестнадцатеричных символов в нижнем регистре.
RID_RE = re.compile(r"[0-9a-f]{8}")
#: 2. путь — начинается с «/», только допустимые URL-символы, без пробелов.
PATH_RE = re.compile(
    r"/[A-Za-z0-9._~!$&'()*+,;=:@%/\-]*"
    r"(?:\?[A-Za-z0-9._~!$&'()*+,;=:@%/?\-]*)?"
)
#: 3. управляющие символы и BOM, подлежащие удалению из входной строки.
CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f\ufeff]")
#: 4. поиск секретов вида token=..., password=... в пути/строке для маскирования.
SECRET_RE = re.compile(
    r"(?<![A-Za-z0-9])(?P<key>token|password|passwd|secret|api[_-]?key)=[^&\s]*",
    re.IGNORECASE,
)

VALID_LEVELS = frozenset({"INFO", "WARN", "ERROR"})
VALID_SERVICES = frozenset({"auth", "orders", "payment"})
VALID_METHODS = frozenset({"GET", "POST", "PUT", "DELETE"})
STATUS_MIN, STATUS_MAX = 100, 599
DURATION_MIN, DURATION_MAX = 0, 600_000
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
ECHO_LIMIT = 200  # сколько символов исходной строки возвращать в ошибке


class LogParseError(ValueError):
    """Строка журнала не прошла разбор. Содержит причину и номер строки."""

    def __init__(self, reason: str, line_no: int | None = None, line: str = ""):
        self.reason = reason
        self.line_no = line_no
        self.line = line
        prefix = f"строка {line_no}: " if line_no is not None else ""
        super().__init__(prefix + reason)

    def as_dict(self) -> dict:
        shown = mask_secrets(str(self.line))[:ECHO_LIMIT]
        return {"line_no": self.line_no, "reason": self.reason, "line": shown}


@dataclass
class ParseBatch:
    valid: list[dict] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)


def clean_line(line: str) -> str:
    """Удаляет управляющие символы/BOM и пробелы по краям."""
    return CONTROL_CHARS_RE.sub("", line).strip()


def mask_secrets(text: str) -> str:
    """Заменяет значения token/password/secret/api_key на ***."""
    return SECRET_RE.sub(lambda m: f"{m.group('key')}=***", text)


def parse_line(line: str, line_no: int | None = None) -> dict:
    """Разбирает одну строку. Возвращает нормализованный словарь или бросает
    :class:`LogParseError` с причиной и номером строки."""

    def fail(reason: str) -> LogParseError:
        return LogParseError(reason, line_no, line if isinstance(line, str) else "")

    if not isinstance(line, str) or not line.strip():
        raise fail("пустая строка")

    match = LINE_RE.fullmatch(clean_line(line))
    if match is None:
        raise fail("строка не соответствует формату журнала")
    g = match.groupdict()

    # timestamp (формат уже проверен маской, значения — через datetime)
    try:
        ts = datetime.strptime(g["timestamp"], TIMESTAMP_FORMAT)
    except ValueError:
        raise fail(f"некорректные дата/время: {g['timestamp']}") from None

    if g["level"] not in VALID_LEVELS:
        raise fail(f"недопустимый уровень: {g['level']}")
    if g["service"] not in VALID_SERVICES:
        raise fail(f"недопустимый сервис: {g['service']}")

    try:
        ip = str(ipaddress.IPv4Address(g["ip"]))
    except ipaddress.AddressValueError:
        raise fail(f"некорректный IPv4-адрес: {g['ip']}") from None

    if g["method"] not in VALID_METHODS:
        raise fail(f"недопустимый HTTP-метод: {g['method']}")
    if PATH_RE.fullmatch(g["path"]) is None:
        raise fail("путь должен начинаться с «/» и содержать допустимые символы")

    status = int(g["status"])
    if not STATUS_MIN <= status <= STATUS_MAX:
        raise fail(f"HTTP-код вне диапазона {STATUS_MIN}–{STATUS_MAX}: {status}")

    duration = int(g["duration_ms"])
    if not DURATION_MIN <= duration <= DURATION_MAX:
        raise fail(
            f"duration_ms вне диапазона {DURATION_MIN}–{DURATION_MAX}: {duration}"
        )

    if RID_RE.fullmatch(g["rid"]) is None:
        raise fail("rid должен состоять из 8 шестнадцатеричных символов (0-9a-f)")

    return {
        "timestamp": ts.strftime(TIMESTAMP_FORMAT),
        "level": g["level"],
        "service": g["service"],
        "ip": ip,
        "method": g["method"],
        "path": mask_secrets(g["path"]),
        "status": status,
        "duration_ms": duration,
        "rid": g["rid"],
    }


def parse_lines(lines: list[str], first_line_no: int = 1) -> ParseBatch:
    """Разбирает пакет строк. Некорректные строки попадают в ``errors`` и
    никогда не попадают в ``valid`` (а значит — и в очередь)."""
    batch = ParseBatch()
    for offset, raw in enumerate(lines):
        number = first_line_no + offset
        try:
            batch.valid.append(parse_line(raw, number))
        except LogParseError as exc:
            batch.errors.append(exc.as_dict())
    return batch
