const RAW_API_BASE = (import.meta.env.VITE_API_BASE || "").trim();

const API_BASE = RAW_API_BASE
  ? RAW_API_BASE.replace(/\/+$/, "")
  : `${window.location.protocol}//${window.location.hostname}:8000`;

function getActor() {
  return (localStorage.getItem("username") || "user-ui").trim() || "user-ui";
}

function normalizeErrorMessage(data) {
  if (!data) return "Request failed";

  if (typeof data === "string") return data;
  if (typeof data?.detail === "string") return data.detail;

  if (Array.isArray(data?.detail)) {
    return data.detail
      .map((item) => {
        if (typeof item === "string") return item;
        if (item?.msg) return item.msg;
        return JSON.stringify(item);
      })
      .join(" | ");
  }

  if (data?.detail && typeof data.detail === "object") {
    if (data.detail.msg) return data.detail.msg;
    return JSON.stringify(data.detail);
  }

  if (typeof data?.message === "string") return data.message;

  try {
    return JSON.stringify(data);
  } catch {
    return "Request failed";
  }
}

async function httpJson(url, options = {}) {
  try {
    const res = await fetch(url, {
      cache: "no-store",
      ...options,
      headers: {
        "Cache-Control": "no-cache, no-store, must-revalidate",
        Pragma: "no-cache",
        Expires: "0",
        "Content-Type": "application/json",
        "x-username": getActor(),
        ...(options.headers || {}),
      },
    });

    const data = await res.json().catch(() => ({}));

    if (!res.ok) {
      throw new Error(normalizeErrorMessage(data));
    }

    return data;
  } catch (err) {
    if (err instanceof Error) {
      throw new Error(err.message || `Không gọi được API: ${url}`);
    }
    throw new Error(`Không gọi được API: ${url}`);
  }
}

export function normalizeSearchResponse(payload) {
  const items = Array.isArray(payload?.items)
    ? payload.items
    : Array.isArray(payload)
    ? payload
    : [];

  const total =
    typeof payload?.total === "number"
      ? payload.total
      : items.length;

  return { total, items };
}

export function listClasses({ category = "all" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/classes`);
  url.searchParams.set("category", category);
  url.searchParams.set("_ts", String(Date.now()));
  return httpJson(url.toString(), { method: "GET" });
}

export function listSubjects({ classID = "", category = "all" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/subjects`);
  if (classID) url.searchParams.set("classID", classID);
  url.searchParams.set("category", category);
  url.searchParams.set("_ts", String(Date.now()));
  return httpJson(url.toString(), { method: "GET" });
}

export function listTopics({ subjectID = "", category = "all" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/topics`);
  if (subjectID) url.searchParams.set("subjectID", subjectID);
  url.searchParams.set("category", category);
  url.searchParams.set("_ts", String(Date.now()));
  return httpJson(url.toString(), { method: "GET" });
}

export function listLessons({ topicID = "", category = "all" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/lessons`);
  if (topicID) url.searchParams.set("topicID", topicID);
  url.searchParams.set("category", category);
  url.searchParams.set("_ts", String(Date.now()));
  return httpJson(url.toString(), { method: "GET" });
}

export function listChunks({
  lessonID = "",
  category = "document",
  limit = 50,
  offset = 0,
  sort = "name",
} = {}) {
  const url = new URL(`${API_BASE}/user/docs`);
  if (lessonID) url.searchParams.set("lessonID", lessonID);
  url.searchParams.set("category", category);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  url.searchParams.set("sort", sort);
  url.searchParams.set("_ts", String(Date.now()));
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
  url.searchParams.set("_ts", String(Date.now()));
  return httpJson(url.toString(), { method: "GET" });
}

export function getDocDetail(chunkID, { category = "document" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/${encodeURIComponent(chunkID)}`);
  url.searchParams.set("category", category);
  url.searchParams.set("_ts", String(Date.now()));
  return httpJson(url.toString(), { method: "GET" });
}

export function getDocView(chunkID, { category = "document" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/${encodeURIComponent(chunkID)}/view`);
  url.searchParams.set("category", category);
  url.searchParams.set("_ts", String(Date.now()));
  return httpJson(url.toString(), { method: "GET" });
}

export function getViewUrl(chunkID, opts = {}) {
  return getDocView(chunkID, opts);
}

export function getDocViewUrl(chunkID, opts = {}) {
  return getDocView(chunkID, opts);
}

export function toggleSave(chunkID, category = "document") {
  const url = new URL(`${API_BASE}/user/docs/saved/${encodeURIComponent(chunkID)}/toggle`);
  url.searchParams.set("category", category);
  return httpJson(url.toString(), { method: "POST" });
}

export function listSaved({ category = "document", limit = 50, offset = 0 } = {}) {
  const url = new URL(`${API_BASE}/user/docs/saved/list`);
  url.searchParams.set("category", category);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  url.searchParams.set("_ts", String(Date.now()));
  return httpJson(url.toString(), { method: "GET" });
}

export function saveSearchHistory(q = "") {
  const url = new URL(`${API_BASE}/user/docs/history/search`);
  if (q) url.searchParams.set("q", q);
  return httpJson(url.toString(), { method: "POST" });
}

export function listSearchHistory({ limit = 5 } = {}) {
  const url = new URL(`${API_BASE}/user/docs/history/search/list`);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("_ts", String(Date.now()));
  return httpJson(url.toString(), { method: "GET" });
}

export function removeSearchHistory(q = "") {
  const url = new URL(`${API_BASE}/user/docs/history/search/item`);
  if (q) url.searchParams.set("q", q);
  return httpJson(url.toString(), { method: "DELETE" });
}

export function clearSearchHistory() {
  const url = new URL(`${API_BASE}/user/docs/history/search/clear`);
  return httpJson(url.toString(), { method: "DELETE" });
}

export function saveViewHistory(chunkID, { category = "document" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/history/view/${encodeURIComponent(chunkID)}`);
  url.searchParams.set("category", category);
  return httpJson(url.toString(), { method: "POST" });
}

export function listViewHistory({ category = "all", limit = 50, offset = 0 } = {}) {
  const url = new URL(`${API_BASE}/user/docs/history/view/list`);
  url.searchParams.set("category", category);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  url.searchParams.set("_ts", String(Date.now()));
  return httpJson(url.toString(), { method: "GET" });
}

export function removeViewHistory(chunkID, { category = "document" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/history/view/${encodeURIComponent(chunkID)}`);
  url.searchParams.set("category", category);
  return httpJson(url.toString(), { method: "DELETE" });
}

export function clearViewHistory({ category = "all" } = {}) {
  const url = new URL(`${API_BASE}/user/docs/history/view/clear`);
  url.searchParams.set("category", category);
  return httpJson(url.toString(), { method: "DELETE" });
}
