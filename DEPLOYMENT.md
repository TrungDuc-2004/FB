# Hướng dẫn Docker Compose (Deploy server)

Tài liệu này để bạn gửi Thầy deploy lên server (Ubuntu).

## 0) Tổng quan

Stack chạy bằng Docker Compose:

- **frontend**: React (Vite build) chạy qua **Nginx** cổng `80`
- **backend**: FastAPI (Uvicorn) cổng `8000`
- **postgres**: PostgreSQL cổng `5432` (chỉ cần mở ra ngoài nếu Thầy muốn truy cập trực tiếp)
- **mongo**: MongoDB cổng `27017` (mặc định KHÔNG mở ra ngoài)
- **neo4j**: Neo4j cổng `7474/7687`
- **minio**: MinIO cổng `9000` (API) và `9001` (console)

Frontend gọi backend qua **reverse proxy**: `http://<domain>/api/...` → `backend:8000/...`.

## 1) Chuẩn bị source trước khi gửi

Khuyến nghị:
- **Xoá** thư mục nặng: `.venv/`, `frontend/node_modules/`, `.git/` (nếu gửi zip)
- Giữ các file Docker: `docker-compose.yml`, `backend/Dockerfile`, `frontend/Dockerfile`, `frontend/nginx.conf`, `docker/*`

File `.dockerignore` đã có để Docker build nhanh hơn.

## 2) Tạo file cấu hình `.env`

Tại thư mục gốc project:

```bash
cp .env.example .env
```

Sửa các giá trị quan trọng:
- `PG_PASSWORD`, `NEO4J_PASSWORD`, `MINIO_ROOT_PASSWORD` (nếu deploy thật)
- `MINIO_PUBLIC_BASE_URL`:
  - Local: `http://localhost:9000`
  - Server: `http://<SERVER_IP>:9000` hoặc domain (vd `https://minio.example.com`)

## 3) Chạy local (để test)

```bash
docker compose --version

docker compose up -d --build

docker compose ps
```

Mở:
- UI: `http://localhost/`
- API health: `http://localhost:8000/health`
- MinIO console: `http://localhost:9001`
- Neo4j browser: `http://localhost:7474`

Tài khoản demo PostgreSQL (do init.sql tạo):
- username: `admin`
- password: `admin`

## 4) Deploy lên Ubuntu server

### 4.1 Cài Docker

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg

# Docker official repo
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo $VERSION_CODENAME) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# (optional) chạy docker không cần sudo
sudo usermod -aG docker $USER
# logout/login lại
```

### 4.2 Mở firewall (tuỳ)

Nếu dùng UFW:

```bash
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Nếu Thầy muốn truy cập MinIO/Neo4j từ ngoài:
sudo ufw allow 9000/tcp
sudo ufw allow 9001/tcp
sudo ufw allow 7474/tcp
sudo ufw allow 7687/tcp

# KHÔNG khuyến nghị mở DB ra ngoài
# sudo ufw allow 5432/tcp
# sudo ufw allow 27017/tcp
```

### 4.3 Chạy

```bash
cd /opt
# copy project vào đây, ví dụ /opt/khoaluan
cd /opt/khoaluan

cp .env.example .env
nano .env   # sửa MINIO_PUBLIC_BASE_URL cho đúng IP/domain

docker compose up -d --build

docker compose ps
```

### 4.4 Xem log / restart

```bash
# xem log backend
docker compose logs -f backend

# restart 1 service
docker compose restart backend

# stop toàn bộ
docker compose down

# stop + xoá luôn data (cẩn thận)
docker compose down -v
```

## 5) Notes quan trọng

### 5.1 MinIO link preview

Backend tạo link view dựa trên:
- `MINIO_PUBLIC_BASE_URL` (ưu tiên)
- nếu không set: tự build từ `MINIO_ENDPOINT` (trong docker là `minio:9000` → trình duyệt không truy cập được)

Vì vậy khi deploy server, **phải set** `MINIO_PUBLIC_BASE_URL` là URL mà browser truy cập được.

### 5.2 Database schema

PostgreSQL container sẽ tự chạy file `docker/postgres/init.sql` ở lần khởi tạo đầu tiên để tạo schema tối thiểu và user demo.

Nếu bạn muốn dùng DB có sẵn, chỉ cần:
- giữ nguyên DB/credentials trong `.env`
- hoặc bỏ mount init.sql và trỏ vào DB khác.

