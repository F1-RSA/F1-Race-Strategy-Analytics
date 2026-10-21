FROM python:3.11.14-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Installed from a throwaway copy so the image has one importable f1_ingest (in site-packages),
# not a second one in the working directory. Tests are excluded by .dockerignore.
COPY pyproject.toml /src/pyproject.toml
COPY f1_ingest /src/f1_ingest
RUN pip install --no-cache-dir /src && rm -rf /src

WORKDIR /app

# Keeps the container running so that `docker compose exec ingestion python -m f1_ingest.cli ...`
# works (README, Kestra). The CLI itself is a one-shot command.
CMD ["sleep", "infinity"]
