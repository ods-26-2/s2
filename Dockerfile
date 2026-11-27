
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY components/ ./components/
COPY data/ppe_catalog.exemplo.json ./data/ppe_catalog.exemplo.json

RUN mkdir -p /app/data/output_s2

ENTRYPOINT ["python", "-m", "components.S2.main"]
