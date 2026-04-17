// src/services/postgreAdminApi.js
const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

async function httpJson(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      ...(options.headers || {}),
      "Content-Type": "application/json",
    },
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || JSON.stringify(data) || "Request failed");
  return data;
}

export function listTables() {
  return httpJson(`${API_BASE}/admin/postgre/tables`, { method: "GET" });
}

export function listColumns(tableName) {
  const t = encodeURIComponent(tableName);
  return httpJson(`${API_BASE}/admin/postgre/tables/${t}/columns`, { method: "GET" });
}

export function listRows(tableName, limit = 200, offset = 0) {
  const t = encodeURIComponent(tableName);
  const url = new URL(`${API_BASE}/admin/postgre/tables/${t}/rows`);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  return httpJson(url.toString(), { method: "GET" });
}

export function getRow(tableName, pk) {
  const t = encodeURIComponent(tableName);
  const p = encodeURIComponent(pk); // pk có thể là "chunk_id::keyword_name"
  return httpJson(`${API_BASE}/admin/postgre/tables/${t}/rows/${p}`, { method: "GET" });
}


export function updateRow(tableName, pk, payload) {
  const t = encodeURIComponent(tableName);
  const p = encodeURIComponent(pk);
  return httpJson(`${API_BASE}/admin/postgre/tables/${t}/rows/${p}`, {
    method: "PUT",
    body: JSON.stringify(payload),
  });
}

function getFilenameFromContentDisposition(value = "") {
  const utf8Match = value.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) return decodeURIComponent(utf8Match[1]);

  const plainMatch = value.match(/filename="?([^";]+)"?/i);
  if (plainMatch?.[1]) return plainMatch[1];

  return "download";
}

async function downloadFile(url) {
  const res = await fetch(url, { method: "GET" });

  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    throw new Error(data?.detail || "Download failed");
  }

  const blob = await res.blob();
  const filename = getFilenameFromContentDisposition(res.headers.get("Content-Disposition") || "");
  const objectUrl = window.URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = objectUrl;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.URL.revokeObjectURL(objectUrl);
}

export function exportAll(format = "csv") {
  const url = new URL(`${API_BASE}/admin/postgre/export`);
  url.searchParams.set("format", format);
  return downloadFile(url.toString());
}

export function exportTable(tableName, format = "csv") {
  const t = encodeURIComponent(tableName);
  const url = new URL(`${API_BASE}/admin/postgre/tables/${t}/export`);
  url.searchParams.set("format", format);
  return downloadFile(url.toString());
}
