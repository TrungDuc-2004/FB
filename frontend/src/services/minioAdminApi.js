const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

function makeUploadId() {
  try {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
      return crypto.randomUUID();
    }
  } catch (error) {
    void error;
  }
  return `upload-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function normalizeProgress(progress = {}) {
  const percent = Number(progress?.percent ?? 0);
  return {
    uploadId: progress?.uploadId || "",
    status: progress?.status || "processing",
    stage: progress?.stage || "preparing",
    stageLabel: progress?.stageLabel || progress?.message || "Đang xử lý",
    message: progress?.message || progress?.stageLabel || "Đang xử lý",
    percent: Number.isFinite(percent) ? Math.max(0, Math.min(100, Math.round(percent))) : 0,
    totalFiles: Number(progress?.totalFiles ?? 1) || 1,
    completedFiles: Number(progress?.completedFiles ?? 0) || 0,
    currentFileIndex: Number(progress?.currentFileIndex ?? 0) || 0,
    currentFileName: progress?.currentFileName || "",
    errors: Array.isArray(progress?.errors) ? progress.errors : [],
  };
}

async function getUploadProgress(uploadId) {
  if (!uploadId) return null;
  const res = await fetch(`${API_BASE}/admin/minio/uploads/progress/${encodeURIComponent(uploadId)}`, {
    method: "GET",
    headers: { "x-user": getActor() },
  });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(await res.text());
  return normalizeProgress(await res.json());
}


function parseErrorPayload(raw) {
  try {
    const obj = JSON.parse(raw || "{}");
    if (obj?.detail) return String(obj.detail);
    return raw || "Không thể kết nối tới server";
  } catch (error) {
    void error;
    return raw || "Không thể kết nối tới server";
  }
}

function uploadWithProgress(url, formData, onProgress) {
  return new Promise((resolve, reject) => {
    const uploadId = formData.get("upload_id") || makeUploadId();
    if (!formData.get("upload_id")) formData.append("upload_id", uploadId);

    let stopped = false;
    let highestPercent = 0;
    let pollTimer = null;
    let xhrFinished = false;
    let settled = false;
    let xhrSucceeded = false;
    let xhrResponseText = "";

    const emit = (payload) => {
      if (typeof onProgress !== "function") return;
      const normalized = normalizeProgress({ uploadId, ...payload });
      if (normalized.percent < highestPercent) {
        normalized.percent = highestPercent;
      } else {
        highestPercent = normalized.percent;
      }
      onProgress(normalized);
    };

    const stopPolling = () => {
      stopped = true;
      if (pollTimer) {
        clearTimeout(pollTimer);
        pollTimer = null;
      }
    };

    const finishSuccess = async () => {
      if (settled) return;
      settled = true;
      stopPolling();

      let parsed = null;
      try {
        parsed = xhrResponseText ? JSON.parse(xhrResponseText) : null;
      } catch (error) {
        reject(error);
        return;
      }

      emit({
        status: "completed",
        stage: "completed",
        stageLabel: "Hoàn tất",
        message: "Hoàn tất",
        percent: 100,
      });
      resolve(parsed);
    };

    const finishError = (message) => {
      if (settled) return;
      settled = true;
      stopPolling();
      reject(new Error(parseErrorPayload(message || "Không thể kết nối tới server")));
    };

    const schedulePoll = (delay = 250) => {
      if (stopped || settled) return;
      if (pollTimer) {
        clearTimeout(pollTimer);
        pollTimer = null;
      }
      pollTimer = window.setTimeout(async () => {
        try {
          const progress = await getUploadProgress(uploadId);
          if (progress) {
            emit(progress);
            if (["completed", "completed_with_errors", "failed"].includes(progress.status)) {
              stopPolling();
              if (xhrFinished) {
                if (xhrSucceeded) {
                  await finishSuccess();
                } else {
                  finishError(xhrResponseText || `HTTP ${xhr.status}`);
                }
              }
              return;
            }
          }
        } catch (error) {
          void error;
        }

        if (xhrFinished) {
          if (xhrSucceeded) {
            await finishSuccess();
          } else {
            finishError(xhrResponseText || `HTTP ${xhr.status}`);
          }
          return;
        }

        schedulePoll(250);
      }, delay);
    };

    const xhr = new XMLHttpRequest();
    xhr.open("POST", url, true);
    xhr.setRequestHeader("x-user", getActor());

    xhr.upload.onprogress = (event) => {
      if (!event.lengthComputable) return;
      const clientPercent = Math.max(0, Math.min(10, Math.round((event.loaded / event.total) * 10)));
      emit({
        status: "uploading",
        stage: "uploading_to_server",
        stageLabel: "Đang gửi file lên server",
        message: "Đang gửi file lên server",
        percent: clientPercent,
      });
    };

    xhr.onerror = () => {
      xhrFinished = true;
      xhrSucceeded = false;
      finishError("Không thể kết nối tới server");
    };

    xhr.onload = () => {
      xhrFinished = true;
      xhrSucceeded = xhr.status >= 200 && xhr.status < 300;
      xhrResponseText = xhr.responseText || "";

      if (!xhrSucceeded) {
        stopPolling();
        finishError(xhrResponseText || `HTTP ${xhr.status}`);
        return;
      }

      schedulePoll(0);
    };

    emit({
      status: "uploading",
      stage: "uploading_to_server",
      stageLabel: "Đang gửi file lên server",
      message: "Đang gửi file lên server",
      percent: 0,
    });
    schedulePoll(250);
    xhr.send(formData);
  });
}

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

export async function uploadFiles(path, files, onProgress) {
  const fd = new FormData();
  fd.append("path", path);
  fd.append("upload_id", makeUploadId());
  for (const f of files) fd.append("files", f);
  return uploadWithProgress(`${API_BASE}/admin/minio/files/`, fd, onProgress);
}

export async function insertItem(path, meta, file, onProgress) {
  const fd = new FormData();
  fd.append("path", path);
  fd.append("upload_id", makeUploadId());
  fd.append("name", meta?.name || "");
  fd.append("meta_json", JSON.stringify(meta || {}));
  if (file) fd.append("file", file);
  return uploadWithProgress(`${API_BASE}/admin/minio/objects/`, fd, onProgress);
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

export async function uploadAuto(path, file, { bookVariant = "", onProgress } = {}) {
  const fd = new FormData();
  fd.append("current_path", path);
  fd.append("upload_id", makeUploadId());
  if (bookVariant) fd.append("book_variant", bookVariant);
  fd.append("file", file);
  return uploadWithProgress(`${API_BASE}/admin/minio/upload-auto/`, fd, onProgress);
}
