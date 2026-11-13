FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

ENV DATA_DIR=/data \
    OPCUA_ENDPOINT=opc.tcp://opcua-source:4840 \
    BACKFILL_INTERVAL=2.0 \
    HISTORY_PAGE_SIZE=50

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
