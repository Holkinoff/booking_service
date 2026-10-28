# --- 1. Базовый слой: системные утилиты и дескриптор зависимостей ---
FROM python:3.11-slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /app/


# --- 2. Слой для тестов: dev-зависимости (pytest, httpx, mock и т.д.) ---
FROM base AS test

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e ".[dev]"

COPY . /app/

CMD ["pytest", "-v"]


# --- 3. Продакшн-образ: ---
FROM base AS production

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e "."

COPY . /app/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
