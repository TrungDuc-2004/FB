// src/services/neoAdminApi.js
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

export function listLabels() {
  return httpJson(`${API_BASE}/admin/neo/labels`, { method: "GET" });
}

export function listNodes(label) {
  const url = new URL(`${API_BASE}/admin/neo/nodes`);
  url.searchParams.set("label", label);
  return httpJson(url.toString(), { method: "GET" });
}

export function getNode(nodeId) {
  return httpJson(`${API_BASE}/admin/neo/nodes/${encodeURIComponent(nodeId)}`, { method: "GET" });
}
