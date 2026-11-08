FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY logpulse ./logpulse
COPY tests ./tests
COPY tools ./tools
COPY data ./data
COPY pytest.ini .

RUN useradd --create-home --uid 10001 appuser && chown -R appuser /app
USER appuser

EXPOSE 8000
# API по умолчанию; worker переопределяет команду в compose.yaml
CMD ["uvicorn", "logpulse.main:app", "--host", "0.0.0.0", "--port", "8000"]
