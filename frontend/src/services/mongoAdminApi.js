// src/services/mongoAdminApi.js
const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

async function httpJson(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      ...(options.headers || {}),
      "Content-Type": "application/json",
      "X-Actor": getActor(),
    },
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || JSON.stringify(data) || "Request failed");
  return data;
}

function getActor() {
  return localStorage.getItem("username") || "admin-ui";
}

export function listCollections() {
  return httpJson(`${API_BASE}/admin/mongo/collections`, { method: "GET" });
}

export function createCollection(name) {
  const n = encodeURIComponent(name);
  return httpJson(`${API_BASE}/admin/mongo/collections/${n}`, { method: "POST" });
}

export function deleteCollection(name) {
  const n = encodeURIComponent(name);
  return httpJson(`${API_BASE}/admin/mongo/collections/${n}`, { method: "DELETE" });
}

export function renameCollection(oldName, newName) {
  const oldN = encodeURIComponent(oldName);
  const url = new URL(`${API_BASE}/admin/mongo/collections/${oldN}/rename`);
  url.searchParams.set("new_name", newName);
  return httpJson(url.toString(), { method: "PUT" });
}

export function listDocuments(collectionName, limit = 50, offset = 0) {
  const url = new URL(`${API_BASE}/admin/mongo/documents`);
  url.searchParams.set("collection_name", collectionName);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  return httpJson(url.toString(), { method: "GET" });
}

export function createDocument(collectionName, doc) {
  const c = encodeURIComponent(collectionName);
  return httpJson(`${API_BASE}/admin/mongo/documents/${c}`, {
    method: "POST",
    body: JSON.stringify(doc),
  });
}

export function updateDocument(collectionName, oid, patch) {
  const c = encodeURIComponent(collectionName);
  const id = encodeURIComponent(oid);
  return httpJson(`${API_BASE}/admin/mongo/documents/${c}/${id}`, {
    method: "PUT",
    body: JSON.stringify(patch),
  });
}

export function deleteDocument(collectionName, oid) {
  const c = encodeURIComponent(collectionName);
  const id = encodeURIComponent(oid);
  return httpJson(`${API_BASE}/admin/mongo/documents/${c}/${id}`, { method: "DELETE" });
}

// ========================= BULK IMPORT =========================
export async function importMetadataXlsx(file, { sync = true, category = "document" } = {}) {
  if (!file) throw new Error("Missing file");

  const url = new URL(`${API_BASE}/admin/mongo/import/xlsx`);
  url.searchParams.set("sync", sync ? "true" : "false");
  url.searchParams.set("category", category);

  const fd = new FormData();
  fd.append("file", file);

  const res = await fetch(url.toString(), {
    method: "POST",
    headers: {
      "X-Actor": getActor(),
    },
    body: fd,
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || JSON.stringify(data) || "Request failed");
  return data;
}
