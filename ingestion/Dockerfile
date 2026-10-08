FROM python:3.11.14-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Placeholder until the ingestion package exists: keeps the container running so
# that `docker compose exec ingestion ...` works. Replaced by the real entry point later.
CMD ["sleep", "infinity"]
