# Backend (FastAPI)
# NOTE: Có cài LibreOffice để hỗ trợ convert DOCX/PPTX -> PDF (doc_preview.py).
# Nếu bạn không cần tính năng preview này, có thể bỏ khối apt-get libreoffice để image nhẹ hơn.

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl ca-certificates \
        libreoffice-writer libreoffice-impress \
        fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# App
COPY . ./

EXPOSE 8000

# Prod command (đủ dùng cho demo/deploy server)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
