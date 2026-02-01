# KhoaLuan - Backend & Frontend

## Cấu trúc

- `backend/` : FastAPI (API cho MinIO/MongoDB/PostgreSQL/Neo4j)
- `frontend/` : React (Vite) Dashboard UI (sidebar + topbar cố định)

## Chạy Backend (FastAPI)

```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# cấu hình môi trường
cp core/config.env.example core/config.env

# chạy API
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

API: http://localhost:8000

## Chạy Frontend (React)

```bash
cd frontend
npm install
npm run dev
```

UI: http://localhost:5173
