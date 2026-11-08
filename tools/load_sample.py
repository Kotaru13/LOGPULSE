"""Загружает data/sample.log в API батчами по 100 строк (только стандартная библиотека).

Использование:  python tools/load_sample.py [путь_к_файлу]
Адрес API:      переменная окружения API_URL (по умолчанию http://127.0.0.1:8000)
"""
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000").rstrip("/")
path = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "data" / "sample.log")
lines = [x for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]
accepted = rejected = 0
for start in range(0, len(lines), 100):
    request = urllib.request.Request(
        f"{API_URL}/ingest",
        data=json.dumps({"lines": lines[start:start + 100]}).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = json.load(response)
    except urllib.error.HTTPError as exc:
        sys.exit(f"HTTP {exc.code}: {exc.read().decode('utf-8', 'replace')}")
    except urllib.error.URLError as exc:
        sys.exit(f"API недоступен: {exc.reason}")
    accepted += body["accepted"]
    rejected += body["rejected"]
    for err in body["errors"]:
        print(f"  строка {start + err['line_no']}: {err['reason']}")
print(f"Прочитано строк: {len(lines)}; принято: {accepted}; отклонено: {rejected}")
