FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# a tiny health script (optional)
HEALTHCHECK --interval=30s --timeout=3s --retries=5 CMD python -c "import socket; s=socket.socket(); s.settimeout(2); s.connect(('db',5432)); print('ok')"

CMD ["python", "-u", "main.py"]
