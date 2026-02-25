const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

function actorHeaders() {
  const username = (localStorage.getItem("username") || "anonymous").trim();
  return { "x-user": username };
}

async function apiGet(path, params = {}) {
  const u = new URL(API_BASE + path);
  Object.entries(params || {}).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    u.searchParams.set(k, String(v));
  });

  const res = await fetch(u.toString(), { headers: { ...actorHeaders() } });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || "API error");
  return data;
}

async function apiPost(path, body) {
  const res = await fetch(API_BASE + path, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...actorHeaders() },
    body: body ? JSON.stringify(body) : "{}",
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || "API error");
  return data;
}

export function getClasses(category = "document") {
  return apiGet("/user/docs/classes", { category });
}

export function getSubjects(classID, category = "document") {
  return apiGet("/user/docs/subjects", { classID, category });
}

export function getTopics(subjectID, category = "document") {
  return apiGet("/user/docs/topics", { subjectID, category });
}

export function getLessons(topicID, category = "document") {
  return apiGet("/user/docs/lessons", { topicID, category });
}

export function getChunks(lessonID, category = "document") {
  return apiGet("/user/docs/chunks", { lessonID, category });
}

export function searchDocs(q, top_k = 25, category = "document") {
  return apiGet("/user/docs/search", { q, top_k, category });
}

export function getChunkDetail(chunkId, category = "document") {
  return apiGet(`/user/docs/${encodeURIComponent(chunkId)}`, { category });
}

export function getChunkViewUrl(chunkId, category = "document") {
  return apiGet(`/user/docs/${encodeURIComponent(chunkId)}/view-url`, { category });
}

export function toggleSave(chunkId, category = "document") {
  return apiPost(`/user/docs/${encodeURIComponent(chunkId)}/save?category=${encodeURIComponent(category)}`);
}

export function getSaved(category = "document") {
  return apiGet("/user/docs/saved/list", { category });
}
