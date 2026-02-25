// src/services/userDocsApi.js
// User UI: browse/search/save/view documents

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

function getActor() {
  return localStorage.getItem("username") || "user-ui";
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
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || JSON.stringify(data) || "Request failed");
  return data;
}

export function listClasses() {
  return httpJson(`${API_BASE}/user/docs/classes`, { method: "GET" });
}

export function listSubjects({ classID = "", category = "document" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/subjects`);
  if (classID) url.searchParams.set("classID", classID);
  url.searchParams.set("category", category);
  return httpJson(url.toString(), { method: "GET" });
}

export function listTopics({ subjectID, category = "document" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/topics`);
  url.searchParams.set("subjectID", subjectID);
  url.searchParams.set("category", category);
  return httpJson(url.toString(), { method: "GET" });
}

export function listLessons({ topicID, category = "document" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/lessons`);
  url.searchParams.set("topicID", topicID);
  url.searchParams.set("category", category);
  return httpJson(url.toString(), { method: "GET" });
}

export function listChunks({ lessonID, category = "document", limit = 50, offset = 0 } = {}) {
  const url = new URL(`${API_BASE}/user/docs/chunks`);
  url.searchParams.set("lessonID", lessonID);
  url.searchParams.set("category", category);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  return httpJson(url.toString(), { method: "GET" });
}

export function searchDocs({
  q = "",
  classID = "",
  subjectID = "",
  topicID = "",
  lessonID = "",
  category = "document",
  limit = 20,
  offset = 0,
} = {}) {
  const url = new URL(`${API_BASE}/user/docs/search`);
  if (q) url.searchParams.set("q", q);
  if (classID) url.searchParams.set("classID", classID);
  if (subjectID) url.searchParams.set("subjectID", subjectID);
  if (topicID) url.searchParams.set("topicID", topicID);
  if (lessonID) url.searchParams.set("lessonID", lessonID);
  url.searchParams.set("category", category);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  return httpJson(url.toString(), { method: "GET" });
}

export function getDocDetail(chunkID, { category = "document" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/${encodeURIComponent(chunkID)}`);
  url.searchParams.set("category", category);
  return httpJson(url.toString(), { method: "GET" });
}

export function getViewUrl(chunkID, { category = "document" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/${encodeURIComponent(chunkID)}/view`);
  url.searchParams.set("category", category);
  return httpJson(url.toString(), { method: "GET" });
}

// Backward-compat: DocumentView.jsx (bản cũ) đang import getDocViewUrl.
// Giữ alias để không bị lỗi trắng trang.
export function getDocViewUrl(chunkID, opts = {}) {
  return getViewUrl(chunkID, opts);
}

export function toggleSave(chunkID) {
  return httpJson(`${API_BASE}/user/docs/${encodeURIComponent(chunkID)}/save`, { method: "POST" });
}

export function listSaved({ category = "document", limit = 50, offset = 0 } = {}) {
  const url = new URL(`${API_BASE}/user/docs/saved/list`);
  url.searchParams.set("category", category);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  return httpJson(url.toString(), { method: "GET" });
}
