"""Тесты парсера: 7 типов ошибок поля + 4 дополнительных регулярных выражения."""
from pathlib import Path

import pytest

from logpulse.parser import (
    CONTROL_CHARS_RE,
    PATH_RE,
    RID_RE,
    SECRET_RE,
    LogParseError,
    clean_line,
    mask_secrets,
    parse_line,
    parse_lines,
)

GOOD = "2026-10-20T12:34:56Z INFO auth 192.0.2.10 POST /api/login 200 43ms rid=ab12cd34"
SAMPLE = Path(__file__).resolve().parent.parent / "data" / "sample.log"


def reason_of(line, line_no=7):
    with pytest.raises(LogParseError) as info:
        parse_line(line, line_no)
    assert info.value.line_no == line_no
    return info.value.reason


# ---- корректная строка и ошибки полей ------------------------------------
def test_valid_line_is_normalized():
    assert parse_line(GOOD) == {
        "timestamp": "2026-10-20T12:34:56Z", "level": "INFO", "service": "auth",
        "ip": "192.0.2.10", "method": "POST", "path": "/api/login",
        "status": 200, "duration_ms": 43, "rid": "ab12cd34",
    }


def test_bad_timestamp_rejected():
    line = GOOD.replace("2026-10-20T12:34:56Z", "2026-13-45T25:61:61Z")
    assert "дата/время" in reason_of(line)


def test_bad_ipv4_rejected():
    assert "IPv4" in reason_of(GOOD.replace("192.0.2.10", "192.0.2.999"))


def test_unsupported_level_rejected():
    assert "уровень" in reason_of(GOOD.replace("INFO", "DEBUG"))


def test_bad_status_rejected():
    assert "HTTP-код" in reason_of(GOOD.replace(" 200 ", " 999 "))


def test_negative_duration_rejected():
    assert "duration_ms" in reason_of(GOOD.replace("43ms", "-5ms"))


def test_bad_rid_rejected():
    assert "rid" in reason_of(GOOD.replace("ab12cd34", "zz12cd3"))


def test_empty_and_blank_lines_rejected():
    assert reason_of("") == "пустая строка"
    assert reason_of("   ") == "пустая строка"


def test_unknown_service_method_and_path_rejected():
    assert "сервис" in reason_of(GOOD.replace(" auth ", " billing "))
    assert "метод" in reason_of(GOOD.replace(" POST ", " PATCH "))
    assert "путь" in reason_of(GOOD.replace(" /api/login ", " api/login "))


def test_extra_text_is_not_accepted_fullmatch():
    assert "формат" in reason_of(GOOD + " extra")


# ---- 4 дополнительных регулярных выражения: по тесту на каждое ------------
def test_regex_rid_re():
    assert RID_RE.fullmatch("ab12cd34")
    for bad in ("AB12CD34", "ab12cd3", "ab12cd345", "gg12cd34"):
        assert RID_RE.fullmatch(bad) is None


def test_regex_path_re():
    assert PATH_RE.fullmatch("/api/orders?limit=5")
    for bad in ("api/orders", "/api/or ders", "/api/<script>"):
        assert PATH_RE.fullmatch(bad) is None


def test_regex_control_chars_re_and_clean_line():
    dirty = "\ufeff" + GOOD + "\x00\x07 "
    assert CONTROL_CHARS_RE.search(dirty)
    assert clean_line(dirty) == GOOD
    assert parse_line(dirty)["rid"] == "ab12cd34"


def test_regex_secret_re_masks_values():
    assert SECRET_RE.search("/api/login?token=abc123")
    masked = mask_secrets("/x?access_token=aaa&password=bbb&api-key=ccc&page=2")
    assert "aaa" not in masked and "bbb" not in masked and "ccc" not in masked
    assert masked.endswith("page=2")
    assert parse_line(GOOD.replace("/api/login", "/api/login?token=s3cr3t"))["path"] == "/api/login?token=***"


# ---- пакеты ----------------------------------------------------------------
def test_mixed_batch_three_valid_two_invalid():
    lines = [GOOD, "мусор", GOOD.replace("ab12cd34", "cd34ab12"),
             GOOD.replace("192.0.2.10", "999.1.1.1"),
             GOOD.replace("ab12cd34", "00ff00ff")]
    batch = parse_lines(lines)
    assert len(batch.valid) == 3 and len(batch.errors) == 2
    assert [e["line_no"] for e in batch.errors] == [2, 4]


def test_sample_log_contract():
    lines = [x for x in SAMPLE.read_text(encoding="utf-8").splitlines() if x]
    batch = parse_lines(lines)
    assert len(lines) >= 40 and len(batch.errors) >= 5
    assert {e["level"] for e in batch.valid} == {"INFO", "WARN", "ERROR"}
    assert {e["service"] for e in batch.valid} == {"auth", "orders", "payment"}
    assert len({e["status"] for e in batch.valid}) >= 4
    rids = [e["rid"] for e in batch.valid]
    assert len(rids) == len(set(rids))
