FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY async_app ./async_app
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "async_app.main"]

