FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /service
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 appuser

COPY app ./app
RUN mkdir -p runtime && chown -R appuser:appuser /service

USER 10001
CMD ["python", "-m", "app.main"]
