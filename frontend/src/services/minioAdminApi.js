const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

function getActor() {
  // tuỳ project bạn lưu username ở đâu thì lấy ở đó
  // fallback để khỏi undefined
  return (
    localStorage.getItem("username") ||
    localStorage.getItem("userName") ||
    "admin"
  );
}

async function httpJson(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      ...(options.headers || {}),
      "Content-Type": "application/json",
      "x-user": getActor(),
    },
  });

  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function minioList(path = "") {
  const url = new URL(`${API_BASE}/admin/minio/list`);
  if (path) url.searchParams.set("path", path);
  return httpJson(url.toString(), { method: "GET" });
}

export async function createFolder(fullPath) {
  return httpJson(`${API_BASE}/admin/minio/folders`, {
    method: "POST",
    body: JSON.stringify({ full_path: fullPath }),
  });
}

export async function renameFolder(oldPath, newPath) {
  // backend bạn đang để PUT "/folders/" (có dấu / cuối) -> gọi đúng để khỏi redirect
  return httpJson(`${API_BASE}/admin/minio/folders/`, {
    method: "PUT",
    body: JSON.stringify({ old_path: oldPath, new_path: newPath }),
  });
}

export async function deleteFolder(path) {
  const url = new URL(`${API_BASE}/admin/minio/folders`);
  url.searchParams.set("path", path);
  return httpJson(url.toString(), { method: "DELETE" });
}

export async function uploadFiles(path, files) {
  const fd = new FormData();
  fd.append("path", path);
  for (const f of files) fd.append("files", f);

  const res = await fetch(`${API_BASE}/admin/minio/files/`, {
    method: "POST",
    body: fd,
    headers: {
      // KHÔNG set Content-Type khi dùng FormData
      "x-user": getActor(),
    },
  });

  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function insertItem(path, meta, file) {
  const fd = new FormData();
  fd.append("path", path);
  fd.append("name", meta?.name || "");
  fd.append("meta_json", JSON.stringify(meta || {}));
  if (file) fd.append("file", file);

  const res = await fetch(`${API_BASE}/admin/minio/objects/`, {
    method: "POST",
    body: fd,
    headers: {
      "x-user": getActor(),
    },
  });

  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function deleteObject(objectKey) {
  // backend đang đặt DELETE "/files" để xoá file theo object_key
  const url = new URL(`${API_BASE}/admin/minio/files`);
  url.searchParams.set("object_key", objectKey);
  return httpJson(url.toString(), { method: "DELETE" });
}

export async function renameObject(object_key, new_name) {
  return httpJson(`${API_BASE}/admin/minio/objects/`, {
    method: "PUT",
    body: JSON.stringify({ object_key, new_name }),
  });
}
