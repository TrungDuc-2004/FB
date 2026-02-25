from fastapi import FastAPI

from fastapi.middleware.cors import CORSMiddleware

from .routers.minio import router as minio_router
from .routers.postgre import router as postgre_router
from .routers.mongo import router as mongo_router
from .routers.neo4j import router as neo_router
from .routers.user_docs import router as user_docs_router


# NOTE: Do NOT connect to Postgres at startup.
# The API should be able to start even if the database is down;
# DB connection errors will surface when the /admin/postgre endpoints are called.
app = FastAPI(title="KLTN Backend")

# ===== CORS (để React gọi API) =====
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def home():
    return "Hello Worlds"


@app.get("/health")
def health():
    return {"ok": True}


app.include_router(minio_router)
app.include_router(postgre_router)
app.include_router(mongo_router)
app.include_router(neo_router)

# User UI APIs
app.include_router(user_docs_router)
