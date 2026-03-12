const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

function getActor() {
  return (localStorage.getItem("username") || "user-ui").trim() || "user-ui";
}

function buildUrl(path, params = {}) {
  const url = new URL(`${API_BASE}${path}`);
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    url.searchParams.set(key, String(value));
  });
  return url.toString();
}

async function httpJson(pathOrUrl, options = {}) {
  const isAbsolute = /^https?:\/\//i.test(pathOrUrl);
  const url = isAbsolute ? pathOrUrl : `${API_BASE}${pathOrUrl}`;
  const headers = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    Pragma: "no-cache",
    Expires: "0",
    "x-user": getActor(),
    ...(options.headers || {}),
  };

  if (options.body !== undefined && !headers["Content-Type"]) {
    headers["Content-Type"] = "application/json";
  }

  const res = await fetch(url, {
    cache: "no-store",
    ...options,
    headers,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || JSON.stringify(data) || "Request failed");
  return data;
}

export function listClasses({ category = "document" } = {}) {
  return httpJson(buildUrl("/user/docs/classes", { category }), { method: "GET" });
}

export function listSubjects({ classID = "", category = "document" } = {}) {
  return httpJson(buildUrl("/user/docs/subjects", { classID, category }), { method: "GET" });
}

export function listTopics({ subjectID = "", category = "document" } = {}) {
  return httpJson(buildUrl("/user/docs/topics", { subjectID, category }), { method: "GET" });
}

export function listLessons({ topicID = "", category = "document" } = {}) {
  return httpJson(buildUrl("/user/docs/lessons", { topicID, category }), { method: "GET" });
}

export function listChunks({ lessonID = "", category = "document", limit = 50, offset = 0, sort = "name" } = {}) {
  return httpJson(buildUrl("/user/docs/chunks", { lessonID, category, limit, offset, sort, _ts: Date.now() }), {
    method: "GET",
  });
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
  return httpJson(
    buildUrl("/user/docs/search", {
      q,
      classID,
      subjectID,
      topicID,
      lessonID,
      category,
      limit,
      offset,
      _ts: Date.now(),
    }),
    { method: "GET" }
  );
}

export function getDocDetail(chunkID, { category = "document" } = {}) {
  return httpJson(buildUrl(`/user/docs/${encodeURIComponent(chunkID)}`, { category, _ts: Date.now() }), {
    method: "GET",
  });
}

export function getChunkDetail(chunkID, category = "document") {
  return getDocDetail(chunkID, { category });
}

export function getViewUrl(chunkID, { category = "document" } = {}) {
  return httpJson(buildUrl(`/user/docs/${encodeURIComponent(chunkID)}/view`, { category, _ts: Date.now() }), {
    method: "GET",
  });
}

export function getDocViewUrl(chunkID, opts = {}) {
  return getViewUrl(chunkID, opts);
}

export function getChunkViewUrl(chunkID, category = "document") {
  return getViewUrl(chunkID, { category });
}

export function toggleSave(chunkID, { category = "document" } = {}) {
  return httpJson(buildUrl(`/user/docs/${encodeURIComponent(chunkID)}/save`, { category }), {
    method: "POST",
    body: JSON.stringify({}),
  });
}

export function listSaved({ category = "document", limit = 50, offset = 0 } = {}) {
  return httpJson(buildUrl("/user/docs/saved/list", { category, limit, offset, _ts: Date.now() }), {
    method: "GET",
  });
}

export function getSaved(category = "document") {
  return listSaved({ category });
}