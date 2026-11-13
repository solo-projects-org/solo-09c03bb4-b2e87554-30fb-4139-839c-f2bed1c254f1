FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .

ENV PORT=4840 \
    CONTROL_PORT=8081 \
    DATA_DIR=/data \
    HISTORY_MAX_PAGE=100

EXPOSE 4840 8081
CMD ["python", "server.py"]
