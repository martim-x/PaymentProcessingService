FROM python:3.15.2-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 payment \
    && useradd --system --uid 10001 --gid payment --create-home --home-dir /home/payment --shell /usr/sbin/nologin payment

COPY pyproject.toml README.md requirements.lock ./
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./

RUN pip install --no-cache-dir --require-hashes -r requirements.lock \
    && pip install --no-cache-dir --no-deps . \
    && chown -R payment:payment /app /home/payment

USER 10001:10001

CMD ["uvicorn", "payment.entrypoints.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
