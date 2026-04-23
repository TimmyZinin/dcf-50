FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates tzdata sqlite3 curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY src /app/src
COPY docs /app/docs

RUN mkdir -p /data
ENV DCF50_DB=/data/state.db \
    EDGAR_TICKER_CACHE=/data/edgar_tickers.json

CMD ["python", "-m", "src.scheduler"]
