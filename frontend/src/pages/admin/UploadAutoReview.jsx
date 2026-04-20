import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import * as minioApi from "../../services/minioAdminApi";
import "../../styles/admin/page.css";

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

function getClassNumber(path = "") {
  const m = String(path || "").match(/class-(\d+)/i);
  return m ? Number(m[1]) : 0;
}

function normalizeModeFromPath(path = "") {
  const parts = String(path || "").split("/").filter(Boolean);
  const tail = String(parts[parts.length - 1] || "").toLowerCase();
  if (tail === "subjects") return "subject";
  if (tail === "topics") return "topic";
  if (tail === "lessons") return "lesson";
  if (tail === "chunks") return "chunk";
  return "";
}

function defaultVariantForClass(classNumber) {
  if (classNumber === 10) return "TH10";
  return "";
}

function normalizeProgress(progress = null) {
  if (!progress) return null;
  return {
    stageLabel: progress.stageLabel || progress.message || "Đang xử lý",
    message: progress.message || progress.stageLabel || "Đang xử lý",
    percent: Number(progress.percent || 0),
    currentFileIndex: Number(progress.currentFileIndex || 0),
    totalFiles: Number(progress.totalFiles || 1),
    currentFileName: progress.currentFileName || "",
    status: progress.status || "processing",
  };
}

function formatKb(size = 0) {
  return `${Math.round((size || 0) / 1024)} KB`;
}

function absUrl(url = "") {
  if (!url) return "";
  if (/^https?:\/\//i.test(url)) return url;
  return `${API_BASE}${url}`;
}

function buildPagePreviewUrl(sessionId, itemId, kind = "current", page = 1) {
  if (!sessionId || !itemId) return "";
  const safePage = Math.max(1, Number(page || 1));
  return `${API_BASE}/admin/minio/upload-auto/session/${encodeURIComponent(sessionId)}/page-preview?item_id=${encodeURIComponent(itemId)}&kind=${encodeURIComponent(kind)}&page=${safePage}`;
}


function normalizeCropBands(raw, fallbackPage = 1, fallbackTop = null, fallbackBottom = null) {
  const list = Array.isArray(raw) ? raw : [];
  const map = new Map();

  for (const entry of list) {
    const page = Number(entry?.page);
    const cropTop = Number(entry?.cropTop);
    const cropBottom = Number(entry?.cropBottom);
    if (!Number.isFinite(page) || !Number.isFinite(cropTop) || !Number.isFinite(cropBottom) || cropBottom <= cropTop) continue;
    map.set(Math.max(1, Math.round(page)), {
      page: Math.max(1, Math.round(page)),
      cropTop: Math.round(cropTop),
      cropBottom: Math.round(cropBottom),
    });
  }

  if (!map.size) {
    const page = Number(fallbackPage || 1);
    const cropTop = Number(fallbackTop);
    const cropBottom = Number(fallbackBottom);
    if (Number.isFinite(page) && Number.isFinite(cropTop) && Number.isFinite(cropBottom) && cropBottom > cropTop) {
      map.set(Math.max(1, Math.round(page)), {
        page: Math.max(1, Math.round(page)),
        cropTop: Math.round(cropTop),
        cropBottom: Math.round(cropBottom),
      });
    }
  }

  return Array.from(map.values()).sort((a, b) => a.page - b.page);
}

function getCropBandForPage(item, page) {
  const bands = normalizeCropBands(item?.cropBands, item?.cropPage, item?.cropTop, item?.cropBottom);
  const targetPage = Math.max(1, Number(page || item?.cropPage || 1));
  return bands.find((band) => band.page === targetPage) || null;
}

function normalizeReviewItem(item) {
  if (!item) return item;
  if (item.kind !== "chunk") return { ...item, approved: item.approved !== false };

  const cropBands = normalizeCropBands(item.cropBands, item.cropPage, item.cropTop, item.cropBottom);
  const selectedPage = Math.max(1, Number(item.cropPage || cropBands[0]?.page || 1));
  const activeBand = cropBands.find((band) => band.page === selectedPage) || null;

  return {
    ...item,
    approved: item.approved !== false,
    cropPage: selectedPage,
    cropTop: activeBand ? activeBand.cropTop : null,
    cropBottom: activeBand ? activeBand.cropBottom : null,
    cropBands,
  };
}

function upsertCropBand(item, page, cropTop, cropBottom) {
  const bands = normalizeCropBands(item?.cropBands, item?.cropPage, item?.cropTop, item?.cropBottom);
  const safePage = Math.max(1, Number(page || 1));
  const next = bands.filter((band) => band.page !== safePage);

  const top = Number(cropTop);
  const bottom = Number(cropBottom);
  if (Number.isFinite(top) && Number.isFinite(bottom) && bottom > top) {
    next.push({ page: safePage, cropTop: Math.round(top), cropBottom: Math.round(bottom) });
  }

  next.sort((a, b) => a.page - b.page);
  const activeBand = next.find((band) => band.page === safePage) || null;

  return {
    cropPage: safePage,
    cropTop: activeBand ? activeBand.cropTop : null,
    cropBottom: activeBand ? activeBand.cropBottom : null,
    cropBands: next,
  };
}

function removeCropBand(item, page) {
  return upsertCropBand(item, page, null, null);
}


function kindLabel(kind = "") {
  if (kind === "topic") return "Topic";
  if (kind === "lesson") return "Lesson";
  if (kind === "chunk") return "Chunk";
  return kind || "Mục";
}

function displayTitle(item) {
  if (!item) return "";
  return item.title || item.heading || item.name || item.reviewId || "";
}

function itemPages(item) {
  if (!item) return "";
  return `Trang ${item.start || "?"} - ${item.end || item.start || "?"}`;
}

function confidenceLabel(item) {
  return item?.confidence === "low" ? "Độ tin cậy thấp" : "Độ tin cậy cao";
}

function confidenceStyle(item) {
  return item?.confidence === "low"
    ? { background: "#fee2e2", color: "#b91c1c", border: "1px solid #fecaca" }
    : { background: "#dcfce7", color: "#166534", border: "1px solid #bbf7d0" };
}

function buildInitialReviewedMap(items = []) {
  const out = {};
  for (const item of items || []) {
    if (!item?.reviewId) continue;
    out[item.reviewId] = item.approved !== false && item.confidence !== "low";
  }
  return out;
}

function normalizeReviewPayload(payload) {
  if (!payload) return null;
  const items = Array.isArray(payload.items)
    ? payload.items.map((item) => normalizeReviewItem(item))
    : [];
  return {
    sessionId: payload.session_id,
    mode: payload.mode || "",
    subjectMap: payload.subject_map || "",
    classMap: payload.class_map || "",
    counts: payload.counts || {},
    items,
  };
}

function clampInt(value, min, max, fallback = min) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.round(parsed)));
}

function ensureStartEnd(start, end, maxValue = 1) {
  const safeMax = Math.max(1, Number(maxValue || 1));
  const nextStart = clampInt(start, 1, safeMax, 1);
  const nextEnd = clampInt(end, nextStart, safeMax, nextStart);
  return { start: nextStart, end: nextEnd };
}

function getParentLabel(item) {
  if (!item) return "Mục cha";
  if (item.kind === "chunk") return "Lesson cha";
  if (item.kind === "lesson") return "Nguồn lesson/topic";
  if (item.kind === "topic") return "Nguồn subject";
  return "Nguồn";
}

function getCurrentPreviewLabel(item) {
  if (!item) return "Preview hiện tại";
  if (item.kind === "chunk") return "Kết quả crop hiện tại";
  if (item.kind === "lesson" || item.kind === "topic") return "Kết quả split hiện tại";
  return "Preview hiện tại";
}

function PreviewMedia({ url, title, preferImage = false, className = "", style = {}, children = null }) {
  const full = absUrl(url);
  if (!full) {
    return <div style={{ ...emptyPreviewStyle, ...style }}>Không có preview</div>;
  }
  const lower = full.toLowerCase();
  const isImage =
    preferImage ||
    /\.(png|jpg|jpeg|webp)(\?|$)/.test(lower) ||
    lower.includes("kind=debug") ||
    lower.includes("kind=top") ||
    lower.includes("kind=bottom");

  return (
    <div className={className} style={{ position: "relative", ...style }}>
      {isImage ? (
        <img src={full} alt={title} style={imgPreviewStyle} draggable={false} />
      ) : (
        <iframe title={title} src={full} style={iframePreviewStyle} />
      )}
      {children}
    </div>
  );
}

export default function UploadAutoReview() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const currentPath = searchParams.get("path") || "";
  const classNumber = getClassNumber(currentPath);
  const mode = normalizeModeFromPath(currentPath);

  const [bookVariant, setBookVariant] = useState(defaultVariantForClass(classNumber));
  const [file, setFile] = useState(null);
  const [busyAnalyze, setBusyAnalyze] = useState(false);
  const [busyRefresh, setBusyRefresh] = useState(false);
  const [busyApprove, setBusyApprove] = useState(false);
  const [error, setError] = useState("");
  const [progress, setProgress] = useState(null);
  const [review, setReview] = useState(null);
  const [selectedId, setSelectedId] = useState("");
  const [reviewedMap, setReviewedMap] = useState({});

  const selectedItem = useMemo(() => {
    const items = review?.items || [];
    return items.find((item) => item.reviewId === selectedId) || items[0] || null;
  }, [review, selectedId]);

  const grouped = useMemo(() => {
    const items = review?.items || [];
    return {
      topics: items.filter((item) => item.kind === "topic"),
      lessons: items.filter((item) => item.kind === "lesson"),
      chunks: items.filter((item) => item.kind === "chunk"),
    };
  }, [review]);

  const allReviewed = useMemo(() => {
    const items = review?.items || [];
    const active = items.filter((item) => item.approved !== false);
    return active.length > 0 && active.every((item) => reviewedMap[item.reviewId]);
  }, [review, reviewedMap]);

  const selectedIndex = useMemo(() => {
    const items = review?.items || [];
    return Math.max(0, items.findIndex((item) => item.reviewId === selectedItem?.reviewId));
  }, [review, selectedItem]);

  const bookCode = useMemo(() => {
    if (classNumber === 10) return "TH10";
    if (bookVariant === "UD") return `TH${classNumber}-UD`;
    if (bookVariant === "KHMT") return `TH${classNumber}-KHMT`;
    return "";
  }, [bookVariant, classNumber]);

  const topicOptions = grouped.topics;
  const lessonOptions = grouped.lessons;

  function replaceItem(nextItem) {
    const normalizedNext = normalizeReviewItem(nextItem);
    setReview((prev) => {
      if (!prev) return prev;
      return {
        ...prev,
        items: (prev.items || []).map((item) => (item.reviewId === normalizedNext.reviewId ? normalizeReviewItem({ ...item, ...normalizedNext }) : item)),
      };
    });
  }

  function updateSelected(patch) {
    if (!selectedItem) return;
    replaceItem({ ...selectedItem, ...patch });
    setReviewedMap((prev) => ({ ...prev, [selectedItem.reviewId]: false }));
  }

  function selectAdjacent(direction) {
    const items = review?.items || [];
    if (!items.length) return;
    const nextIndex = Math.min(items.length - 1, Math.max(0, selectedIndex + direction));
    setSelectedId(items[nextIndex].reviewId);
  }

  async function handleAnalyze(e) {
    e?.preventDefault?.();
    if (!file) {
      setError("Hãy chọn file PDF.");
      return;
    }
    if (classNumber !== 10 && !bookVariant) {
      setError("Hãy chọn sách.");
      return;
    }

    setBusyAnalyze(true);
    setError("");
    setProgress({ stageLabel: "Đang gửi file lên server", message: "Đang gửi file lên server", percent: 0, status: "uploading" });

    try {
      const res = await minioApi.prepareUploadAutoReview(currentPath, file, {
        bookVariant: classNumber === 10 ? "" : bookVariant,
        onProgress: (payload) => setProgress(normalizeProgress(payload)),
      });
      const normalized = normalizeReviewPayload(res);
      setReview(normalized);
      setSelectedId(normalized?.items?.[0]?.reviewId || "");
      setReviewedMap(buildInitialReviewedMap(normalized?.items || []));
    } catch (err) {
      setError(String(err?.message || err));
    } finally {
      setBusyAnalyze(false);
    }
  }

  async function handleRefreshItem({ autoMode = false, patch = null } = {}) {
    if (!review?.sessionId || !selectedItem) return;
    setBusyRefresh(true);
    setError("");
    try {
      const target = patch ? { ...selectedItem, ...patch } : selectedItem;
      const payload = {
        ...target,
        yLine: autoMode ? null : target.yLine,
        cropTop: autoMode ? null : target.cropTop,
        cropBottom: autoMode ? null : target.cropBottom,
        cropPage: autoMode ? 1 : target.cropPage,
        cropBands: autoMode ? [] : normalizeCropBands(target.cropBands, target.cropPage, target.cropTop, target.cropBottom),
      };
      const res = await minioApi.updateUploadAutoReviewItem(review.sessionId, payload, {
        onProgress: (p) => setProgress(normalizeProgress(p)),
      });
      if (res?.item) {
        replaceItem(res.item);
        setSelectedId(res.item.reviewId);
        setReviewedMap((prev) => ({ ...prev, [res.item.reviewId]: false }));
      }
    } catch (err) {
      setError(String(err?.message || err));
    } finally {
      setBusyRefresh(false);
    }
  }

  async function handleApprove() {
    if (!review?.sessionId) return;
    if (!allReviewed) {
      setError("Bạn cần duyệt xong từng mục đang bật sync trước khi bắt đầu sync.");
      return;
    }
    setBusyApprove(true);
    setError("");
    try {
      await minioApi.approveUploadAuto(review.sessionId, review.items || [], {
        onProgress: (payload) => setProgress(normalizeProgress(payload)),
      });
      alert("Đã duyệt và sync xong dữ liệu.");
      navigate("/admin/minio");
    } catch (err) {
      setError(String(err?.message || err));
    } finally {
      setBusyApprove(false);
    }
  }

  return (
    <div className="minio-page upload-auto-page-scroll upload-auto-page-shell">
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <h2 className="page-title">Upload tự động và duyệt kết quả cắt</h2>
            <button className="back-btn back-btn-right" onClick={() => navigate(-1)}>
              Quay lại
            </button>
          </div>
          {currentPath ? <div className="breadcrumb">{currentPath}</div> : null}
        </div>
      </div>

      {!review?.sessionId ? (
        <form className="table-wrapper" onSubmit={handleAnalyze} style={{ padding: 20, overflow: "visible" }}>
          <div className="upload-auto-intro-grid">
            <div>
              <label style={labelStyle}>Đường dẫn hiện tại</label>
              <input value={currentPath} readOnly style={inputStyle} />
            </div>
            <div>
              <label style={labelStyle}>Loại upload auto</label>
              <input value={mode || "-"} readOnly style={inputStyle} />
            </div>
            <div>
              <label style={labelStyle}>Sách</label>
              {classNumber === 10 ? (
                <input value="TH10" readOnly style={inputStyle} />
              ) : (
                <>
                  <select value={bookVariant} onChange={(e) => setBookVariant(e.target.value)} style={inputStyle} disabled={busyAnalyze}>
                    <option value="">Chọn sách</option>
                    <option value="UD">Ứng dụng (UD)</option>
                    <option value="KHMT">Khoa học máy tính (KHMT)</option>
                  </select>
                  <input value={bookCode} readOnly style={{ ...inputStyle, marginTop: 8 }} placeholder="Mã sách tự suy ra" />
                </>
              )}
            </div>
            <div>
              <label style={labelStyle}>Chọn file PDF</label>
              <input
                type="file"
                accept="application/pdf,.pdf"
                disabled={busyAnalyze}
                onChange={(e) => {
                  setFile(e.target.files?.[0] || null);
                  setError("");
                }}
                style={inputStyle}
              />
              {file ? (
                <div style={{ marginTop: 8, fontSize: 13, color: "#334155" }}>
                  <div><strong>File:</strong> {file.name}</div>
                  <div><strong>Dung lượng:</strong> {formatKb(file.size)}</div>
                </div>
              ) : null}
            </div>
          </div>

          <div style={{ marginTop: 16, display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
            <button className="btn btn-primary" type="submit" disabled={busyAnalyze || !file}>
              {busyAnalyze ? "Đang cắt tự động..." : "Bắt đầu upload tự động"}
            </button>
            <div style={{ color: "#475569", fontSize: 14 }}>
              Sau khi hệ thống cắt xong, trang này sẽ hiện kết quả để bạn duyệt từng mục.
            </div>
          </div>

          {progress ? <ProgressBox progress={progress} /> : null}
          {error ? <div style={errorStyle}>{error}</div> : null}
        </form>
      ) : (
        <>
          <div className="table-wrapper" style={{ marginBottom: 20, padding: 20, overflow: "visible" }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "flex-start", flexWrap: "wrap" }}>
              <div>
                <h3 style={{ marginTop: 0, marginBottom: 8 }}>Kết quả cắt đã có sẵn để duyệt</h3>
              </div>
              <div className="upload-auto-stats-grid">
                <StatCard label="Topic" value={review.counts?.topics || 0} />
                <StatCard label="Lesson" value={review.counts?.lessons || 0} />
                <StatCard label="Chunk" value={review.counts?.chunks || 0} />
                <StatCard label="Độ tin cậy cao" value={review.counts?.highConfidence || 0} />
                <StatCard label="Độ tin cậy thấp" value={review.counts?.lowConfidence || 0} />
              </div>
            </div>
            {progress ? <ProgressBox progress={progress} /> : null}
            {error ? <div style={errorStyle}>{error}</div> : null}
          </div>

          <div className="upload-auto-top-grid">
            <div className="table-wrapper upload-auto-review-panel upload-auto-left-panel" style={{ padding: 16 }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12, gap: 8, flexWrap: "wrap" }}>
                <strong>Danh sách mục cần duyệt</strong>
                <span style={{ color: "#64748b", fontSize: 13 }}>
                  {Object.values(reviewedMap).filter(Boolean).length}/{(review.items || []).filter((item) => item.approved !== false).length} đã duyệt
                </span>
              </div>
              <div className="upload-auto-left-scroll">
                {[
                  { key: "topics", label: "Topic", rows: grouped.topics },
                  { key: "lessons", label: "Lesson", rows: grouped.lessons },
                  { key: "chunks", label: "Chunk", rows: grouped.chunks },
                ].map((group) =>
                  group.rows.length ? (
                    <div key={group.key} style={{ marginBottom: 16 }}>
                      <div style={{ fontWeight: 700, marginBottom: 8 }}>{group.label}</div>
                      <div style={{ display: "grid", gap: 10 }}>
                        {group.rows.map((item) => {
                          const selected = selectedItem?.reviewId === item.reviewId;
                          const reviewed = Boolean(reviewedMap[item.reviewId]);
                          return (
                            <button
                              key={item.reviewId}
                              type="button"
                              onClick={() => setSelectedId(item.reviewId)}
                              className={`upload-auto-item-card ${selected ? "is-selected" : ""}`}
                            >
                              <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
                                <div style={{ fontWeight: 800, color: "#111827" }}>{kindLabel(item.kind)} {item.number ? `${item.number}.` : ""}</div>
                                <span style={{ ...smallPillStyle, ...confidenceStyle(item) }}>{confidenceLabel(item)}</span>
                              </div>
                              <div className="upload-auto-item-title">{displayTitle(item) || "(chưa có tiêu đề)"}</div>
                              <div style={{ marginTop: 6, color: "#64748b", fontSize: 13 }}>{itemPages(item)}</div>
                              <div style={{ marginTop: 6, color: reviewed ? "#15803d" : "#b45309", fontWeight: 700, fontSize: 13 }}>
                                {reviewed ? "Đã duyệt" : "Chưa duyệt"}
                              </div>
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  ) : null
                )}
              </div>
            </div>

            <div className="table-wrapper upload-auto-review-panel upload-auto-middle-panel" style={{ padding: 20 }}>
              {selectedItem ? (
                <>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "flex-start", flexWrap: "wrap" }}>
                    <div>
                      <h3 style={{ marginTop: 0, marginBottom: 6 }}>{kindLabel(selectedItem.kind)} đang duyệt</h3>
                      <div style={{ color: "#475569", fontSize: 15, lineHeight: 1.45 }}>{displayTitle(selectedItem) || "(chưa có tiêu đề)"}</div>
                      <div style={{ color: "#64748b", marginTop: 6 }}>{itemPages(selectedItem)}</div>
                    </div>
                    <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
                      <span style={{ ...smallPillStyle, ...confidenceStyle(selectedItem) }}>{confidenceLabel(selectedItem)}</span>
                      <label style={{ display: "flex", alignItems: "center", gap: 8, fontWeight: 600 }}>
                        <input type="checkbox" checked={selectedItem.approved !== false} onChange={(e) => updateSelected({ approved: e.target.checked })} />
                        Sync
                      </label>
                    </div>
                  </div>

                  <div className="upload-auto-edit-grid">
                    <Field label="Trang bắt đầu">
                      <input
                        type="number"
                        min={1}
                        value={selectedItem.start ?? ""}
                        onChange={(e) => {
                          const nextStart = clampInt(e.target.value || 1, 1, Number(selectedItem.totalPages || 9999), 1);
                          const next = ensureStartEnd(nextStart, selectedItem.end, Number(selectedItem.totalPages || 9999));
                          updateSelected(next);
                        }}
                        style={inputStyle}
                      />
                    </Field>
                    <Field label="Trang kết thúc">
                      <input
                        type="number"
                        min={Number(selectedItem.start || 1)}
                        value={selectedItem.end ?? ""}
                        onChange={(e) => {
                          const next = ensureStartEnd(selectedItem.start, e.target.value || selectedItem.start || 1, Number(selectedItem.totalPages || 9999));
                          updateSelected(next);
                        }}
                        style={inputStyle}
                      />
                    </Field>
                    <Field label="Số">
                      <input value={selectedItem.number ?? ""} onChange={(e) => updateSelected({ number: e.target.value })} style={inputStyle} />
                    </Field>
                    {selectedItem.kind === "chunk" ? (
                      <Field label="Số trang chunk">
                        <input value={selectedItem.chunkPages || Math.max(1, Number(selectedItem.end || 1) - Number(selectedItem.start || 1) + 1)} readOnly style={{ ...inputStyle, background: "#f8fafc" }} />
                      </Field>
                    ) : (
                      <Field label="Tổng trang nguồn">
                        <input value={selectedItem.totalPages || "-"} readOnly style={{ ...inputStyle, background: "#f8fafc" }} />
                      </Field>
                    )}
                  </div>

                  <div style={{ marginTop: 14, display: "grid", gridTemplateColumns: "1fr", gap: 14 }}>
                    <Field label="Heading">
                      <input value={selectedItem.heading || ""} onChange={(e) => updateSelected({ heading: e.target.value })} style={inputStyle} />
                    </Field>
                    <Field label="Title">
                      <input value={selectedItem.title || ""} onChange={(e) => updateSelected({ title: e.target.value })} style={inputStyle} />
                    </Field>
                  </div>

                  {selectedItem.kind === "lesson" ? (
                    <div style={{ marginTop: 14 }}>
                      <Field label="Topic cha">
                        <select value={selectedItem.topicReviewId || ""} onChange={(e) => updateSelected({ topicReviewId: e.target.value })} style={inputStyle}>
                          <option value="">Chọn topic</option>
                          {topicOptions.map((topic) => (
                            <option key={topic.reviewId} value={topic.reviewId}>{displayTitle(topic)}</option>
                          ))}
                        </select>
                      </Field>
                    </div>
                  ) : null}

                  {selectedItem.kind === "chunk" ? (
                    <div style={{ marginTop: 14, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
                      <Field label="Lesson cha">
                        <select value={selectedItem.lessonReviewId || ""} onChange={(e) => updateSelected({ lessonReviewId: e.target.value })} style={inputStyle}>
                          <option value="">Chọn lesson</option>
                          {lessonOptions.map((lesson) => (
                            <option key={lesson.reviewId} value={lesson.reviewId}>{displayTitle(lesson)}</option>
                          ))}
                        </select>
                      </Field>
                      <Field label="Content head">
                        <select value={selectedItem.contentHead ? "1" : "0"} onChange={(e) => updateSelected({ contentHead: e.target.value === "1" })} style={inputStyle}>
                          <option value="0">false</option>
                          <option value="1">true</option>
                        </select>
                      </Field>
                    </div>
                  ) : null}

                  {selectedItem.kind === "chunk" ? (
                    <ChunkCropEditor
                      sessionId={review?.sessionId}
                      item={selectedItem}
                      busy={busyRefresh || busyApprove}
                      onChange={updateSelected}
                      onRefresh={() => handleRefreshItem()}
                      onAutoRefresh={() => handleRefreshItem({ autoMode: true })}
                    />
                  ) : (
                    <SplitRangeEditor
                      item={selectedItem}
                      busy={busyRefresh || busyApprove}
                      onChange={updateSelected}
                      onRefresh={() => handleRefreshItem()}
                    />
                  )}

                  <div style={{ marginTop: 18, display: "flex", flexWrap: "wrap", gap: 12 }}>
                    <button className="btn btn-primary" type="button" onClick={() => handleRefreshItem()} disabled={busyRefresh || busyApprove}>
                      {busyRefresh ? "Đang cập nhật preview..." : selectedItem.kind === "chunk" ? "Lưu crop và cập nhật preview" : "Lưu split và cập nhật preview"}
                    </button>
                    <button className="btn" type="button" onClick={() => setReviewedMap((prev) => ({ ...prev, [selectedItem.reviewId]: !prev[selectedItem.reviewId] }))}>
{reviewedMap[selectedItem.reviewId] ? "Hủy đánh dấu" : "Đánh dấu tài liệu"}
                    </button>
                    <button className="btn" type="button" onClick={() => selectAdjacent(-1)}>Mục trước</button>
                    <button className="btn" type="button" onClick={() => selectAdjacent(1)}>Mục sau</button>
                  </div>
                </>
              ) : (
                <div>Chưa có mục nào để duyệt.</div>
              )}
            </div>
          </div>

          <div className="table-wrapper upload-auto-bottom-preview" style={{ marginTop: 18, padding: 20 }}>
            {selectedItem ? (
              <div className="upload-auto-preview-grid">
                <PreviewCard title={getParentLabel(selectedItem)} subtitle={selectedItem.kind === "chunk" ? "Preview full file lesson cha để đối chiếu khi crop chunk." : "Preview nguồn để đối chiếu trước khi split."}>
                  <PreviewMedia url={selectedItem.previewContextUrl} title="preview-context" style={{ minHeight: 560 }} />
                </PreviewCard>

                <PreviewCard title={getCurrentPreviewLabel(selectedItem)} subtitle={selectedItem.kind === "chunk" ? "Preview file chunk hiện tại sau crop." : "Preview file đã split từ trang bắt đầu đến trang kết thúc."}>
                  <PreviewMedia url={selectedItem.previewCurrentUrl} title="preview-current" style={{ minHeight: 560 }} />
                </PreviewCard>
              </div>
            ) : (
              <div>Chưa có preview.</div>
            )}
          </div>

          <div className="table-wrapper" style={{ marginTop: 20, display: "flex", justifyContent: "space-between", alignItems: "center", gap: 16, flexWrap: "wrap", padding: 20, overflow: "visible" }}>
            <button className="btn btn-primary" type="button" onClick={handleApprove} disabled={!allReviewed || busyApprove || busyRefresh}>
              {busyApprove ? "Đang sync..." : allReviewed ? "Duyệt xong và bắt đầu sync" : "Hãy duyệt hết từng mục trước"}
            </button>
          </div>
        </>
      )}
    </div>
  );
}


function ChunkCropEditor({ sessionId, item, busy, onChange, onRefresh, onAutoRefresh }) {
  const frameRef = useRef(null);
  const imgRef = useRef(null);
  const [draggingEdge, setDraggingEdge] = useState("");
  const [renderBox, setRenderBox] = useState({ width: 0, height: 0, naturalHeight: 0 });

  const chunkPages = Math.max(1, Number(item.chunkPages || (Number(item.end || 1) - Number(item.start || 1) + 1) || 1));
  const cropBands = useMemo(
    () => normalizeCropBands(item.cropBands, item.cropPage, item.cropTop, item.cropBottom),
    [item.cropBands, item.cropPage, item.cropTop, item.cropBottom]
  );
  const cropPage = Math.max(1, Math.min(chunkPages, Number(item.cropPage || cropBands[0]?.page || 1)));
  const currentBand = cropBands.find((band) => band.page === cropPage) || null;
  const pagePreviewUrl = buildPagePreviewUrl(sessionId, item.reviewId, "current", cropPage);

  const syncMeasure = useCallback(() => {
    const frame = frameRef.current;
    const img = imgRef.current;
    if (!frame || !img) return;
    const rect = frame.getBoundingClientRect();
    setRenderBox({
      width: rect.width,
      height: rect.height,
      naturalHeight: Number(img.naturalHeight || 0),
    });
  }, []);

  const safeTop = currentBand?.cropTop == null ? Math.round(renderBox.naturalHeight * 0.2) : Number(currentBand.cropTop || 0);
  const safeBottom = currentBand?.cropBottom == null
    ? Math.round(renderBox.naturalHeight * 0.8)
    : Number(currentBand.cropBottom || renderBox.naturalHeight || 0);

  const cropTop = Math.max(0, Math.min(safeTop, Math.max(0, safeBottom - 1)));
  const cropBottom = Math.max(cropTop + 1, Math.min(safeBottom, renderBox.naturalHeight || safeBottom || 1));

  const applyBandPatch = useCallback((top, bottom, page = cropPage) => {
    onChange(upsertCropBand(item, page, top, bottom));
  }, [cropPage, item, onChange]);

  const switchCropPage = useCallback((page) => {
    const safePage = Math.max(1, Math.min(chunkPages, Number(page || 1)));
    const band = getCropBandForPage(item, safePage);
    onChange({
      cropPage: safePage,
      cropTop: band ? band.cropTop : null,
      cropBottom: band ? band.cropBottom : null,
      cropBands: normalizeCropBands(item.cropBands, item.cropPage, item.cropTop, item.cropBottom),
    });
  }, [chunkPages, item, onChange]);

  const clearCurrentPage = useCallback(() => {
    onChange(removeCropBand(item, cropPage));
  }, [cropPage, item, onChange]);

  const clearAllPages = useCallback(() => {
    onChange({ cropPage: 1, cropTop: null, cropBottom: null, cropBands: [], yLine: null });
  }, [onChange]);

  const syncLineFromPointer = useCallback((clientY, edge) => {
    const frame = frameRef.current;
    if (!frame || !renderBox.height || !renderBox.naturalHeight || !edge) return;
    const rect = frame.getBoundingClientRect();
    const y = Math.max(0, Math.min(rect.height, clientY - rect.top));
    const naturalY = Math.round((y / rect.height) * renderBox.naturalHeight);
    if (edge === "top") {
      applyBandPatch(Math.max(0, Math.min(naturalY, cropBottom - 1)), cropBottom, cropPage);
      return;
    }
    applyBandPatch(cropTop, Math.max(cropTop + 1, Math.min(naturalY, renderBox.naturalHeight)), cropPage);
  }, [applyBandPatch, cropBottom, cropPage, cropTop, renderBox.height, renderBox.naturalHeight]);

  useEffect(() => {
    function handleMove(e) {
      if (!draggingEdge) return;
      e.preventDefault();
      syncLineFromPointer(e.clientY, draggingEdge);
    }

    function handleUp() {
      setDraggingEdge("");
    }

    window.addEventListener("mousemove", handleMove);
    window.addEventListener("mouseup", handleUp);
    return () => {
      window.removeEventListener("mousemove", handleMove);
      window.removeEventListener("mouseup", handleUp);
    };
  }, [draggingEdge, syncLineFromPointer]);

  useEffect(() => {
    const timer = window.setTimeout(syncMeasure, 60);
    window.addEventListener("resize", syncMeasure);
    return () => {
      window.clearTimeout(timer);
      window.removeEventListener("resize", syncMeasure);
    };
  }, [item?.reviewId, pagePreviewUrl, syncMeasure]);

  const lineTopPct = renderBox.naturalHeight ? `${(cropTop / renderBox.naturalHeight) * 100}%` : "20%";
  const lineBottomPct = renderBox.naturalHeight ? `${(cropBottom / renderBox.naturalHeight) * 100}%` : "80%";
  const cropModeText = currentBand
    ? `Trang ${cropPage} · crop từ ${currentBand.cropTop} đến ${currentBand.cropBottom}`
    : `Trang ${cropPage} · chưa có crop tay`;

  return (
    <div className="upload-auto-editor-block">
      <div className="upload-auto-section-head">
        <div>
          <div className="upload-auto-section-title">Crop chunk kiểu 2 đầu</div>
          <div className="upload-auto-section-desc">
            Crop trực tiếp trên preview của chunk. Mỗi trang có vùng crop riêng, lưu trang nào sẽ giữ lại trang đó.
          </div>
        </div>
        <div className="upload-auto-inline-note">{cropModeText}</div>
      </div>

      <div className="upload-auto-crop-shell">
        <div className="upload-auto-crop-toolbar">
          <div className="upload-auto-crop-group">
            <span className="upload-auto-crop-label">Trang chunk</span>
            <select value={cropPage} onChange={(e) => switchCropPage(Number(e.target.value))} style={toolbarInputStyle} disabled={busy}>
              {Array.from({ length: chunkPages }, (_, idx) => idx + 1).map((page) => (
                <option key={page} value={page}>Trang {page}</option>
              ))}
            </select>
          </div>
          <div className="upload-auto-crop-group">
            <span className="upload-auto-crop-label">Trên</span>
            <input
              type="number"
              value={currentBand?.cropTop ?? ""}
              onChange={(e) => applyBandPatch(e.target.value === "" ? null : Number(e.target.value), currentBand?.cropBottom ?? cropBottom, cropPage)}
              style={toolbarInputStyle}
              disabled={busy}
            />
          </div>
          <div className="upload-auto-crop-group">
            <span className="upload-auto-crop-label">Dưới</span>
            <input
              type="number"
              value={currentBand?.cropBottom ?? ""}
              onChange={(e) => applyBandPatch(currentBand?.cropTop ?? cropTop, e.target.value === "" ? null : Number(e.target.value), cropPage)}
              style={toolbarInputStyle}
              disabled={busy}
            />
          </div>
          <button type="button" className="btn" onClick={() => applyBandPatch(Math.max(0, cropTop - 10), cropBottom, cropPage)} disabled={busy}>Trên -10</button>
          <button type="button" className="btn" onClick={() => applyBandPatch(cropTop + 10, cropBottom, cropPage)} disabled={busy}>Trên +10</button>
          <button type="button" className="btn" onClick={() => applyBandPatch(cropTop, Math.max(cropTop + 1, cropBottom - 10), cropPage)} disabled={busy}>Dưới -10</button>
          <button type="button" className="btn" onClick={() => applyBandPatch(cropTop, cropBottom + 10, cropPage)} disabled={busy}>Dưới +10</button>
          <button type="button" className="btn" onClick={clearCurrentPage} disabled={busy}>Xóa crop trang này</button>
          <button type="button" className="btn" onClick={clearAllPages} disabled={busy}>Xóa tất cả crop</button>
          <button type="button" className="btn" onClick={onAutoRefresh} disabled={busy}>Auto</button>
          <button type="button" className="btn btn-primary" onClick={onRefresh} disabled={busy}>Lưu crop và cập nhật preview</button>
        </div>

        <div className="upload-auto-crop-page-note">
          Preview chunk đang chọn · kéo đường đỏ và xanh để lấy phần giữa. Các trang đã lưu crop:{" "}
          {cropBands.length ? cropBands.map((band) => `Trang ${band.page}`).join(", ") : "chưa có"}
        </div>

        <div
          ref={frameRef}
          className="upload-auto-crop-frame"
          onMouseDown={(e) => {
            if (busy) return;
            const frame = frameRef.current;
            if (!frame || !renderBox.naturalHeight) return;
            const rect = frame.getBoundingClientRect();
            const y = e.clientY - rect.top;
            const topPx = renderBox.height ? (cropTop / renderBox.naturalHeight) * renderBox.height : 0;
            const bottomPx = renderBox.height ? (cropBottom / renderBox.naturalHeight) * renderBox.height : 0;
            const picked = Math.abs(y - topPx) <= Math.abs(y - bottomPx) ? "top" : "bottom";
            setDraggingEdge(picked);
            syncLineFromPointer(e.clientY, picked);
          }}
        >
          <img
            ref={imgRef}
            src={pagePreviewUrl}
            alt="chunk-page-preview"
            className="upload-auto-crop-image"
            onLoad={syncMeasure}
            draggable={false}
          />
          <div className="upload-auto-crop-band" style={{ top: lineTopPct, height: renderBox.naturalHeight ? `${((cropBottom - cropTop) / renderBox.naturalHeight) * 100}%` : "60%" }} />
          <div className="upload-auto-crop-line upload-auto-crop-line-top" style={{ top: lineTopPct }} />
          <div className="upload-auto-crop-line upload-auto-crop-line-bottom" style={{ top: lineBottomPct }} />
          <button
            type="button"
            className="upload-auto-crop-handle upload-auto-crop-handle-top"
            style={{ top: lineTopPct }}
            onMouseDown={(e) => {
              e.preventDefault();
              e.stopPropagation();
              setDraggingEdge("top");
            }}
          >
            Trên
          </button>
          <button
            type="button"
            className="upload-auto-crop-handle upload-auto-crop-handle-bottom"
            style={{ top: lineBottomPct }}
            onMouseDown={(e) => {
              e.preventDefault();
              e.stopPropagation();
              setDraggingEdge("bottom");
            }}
          >
            Dưới
          </button>
        </div>
      </div>
    </div>
  );
}

function SplitRangeEditor({ item, busy, onChange, onRefresh }) {
  const totalPages = Math.max(1, Number(item.totalPages || item.end || 1));
  const range = ensureStartEnd(item.start, item.end, totalPages);

  function updateRange(nextStart, nextEnd) {
    const next = ensureStartEnd(nextStart, nextEnd, totalPages);
    onChange(next);
  }

  const selectionLeft = `${((range.start - 1) / totalPages) * 100}%`;
  const selectionWidth = `${((Math.max(1, range.end - range.start + 1)) / totalPages) * 100}%`;

  return (
    <div className="upload-auto-editor-block">
      <div className="upload-auto-section-head">
        <div>
          <div className="upload-auto-section-title">Split {item.kind}</div>
          <div className="upload-auto-section-desc">Kéo thanh trang bắt đầu và trang kết thúc để đổi phạm vi split cho topic hoặc lesson.</div>
        </div>
        <div className="upload-auto-inline-note">Đang chọn từ trang {range.start} đến trang {range.end}</div>
      </div>

      <div className="upload-auto-range-shell">
        <div className="upload-auto-range-bar">
          <div className="upload-auto-range-track" />
          <div className="upload-auto-range-selection" style={{ left: selectionLeft, width: selectionWidth }} />
          <input
            className="upload-auto-range-input upload-auto-range-input-start"
            type="range"
            min={1}
            max={totalPages}
            value={range.start}
            onChange={(e) => updateRange(Number(e.target.value), Math.max(Number(e.target.value), range.end))}
            disabled={busy}
          />
          <input
            className="upload-auto-range-input upload-auto-range-input-end"
            type="range"
            min={1}
            max={totalPages}
            value={range.end}
            onChange={(e) => updateRange(Math.min(range.start, Number(e.target.value)), Number(e.target.value))}
            disabled={busy}
          />
        </div>

        <div className="upload-auto-range-values">
          <div className="upload-auto-range-box">
            <div className="upload-auto-range-label">Bắt đầu</div>
            <div className="upload-auto-range-value">{range.start}</div>
          </div>
          <div className="upload-auto-range-box">
            <div className="upload-auto-range-label">Kết thúc</div>
            <div className="upload-auto-range-value">{range.end}</div>
          </div>
          <div className="upload-auto-range-box">
            <div className="upload-auto-range-label">Tổng trang nguồn</div>
            <div className="upload-auto-range-value">{totalPages}</div>
          </div>
        </div>

        <div className="upload-auto-split-actions">
          <button type="button" className="btn" onClick={() => updateRange(range.start - 1, range.end)} disabled={busy || range.start <= 1}>Lùi đầu</button>
          <button type="button" className="btn" onClick={() => updateRange(range.start + 1, range.end)} disabled={busy || range.start >= range.end}>Tăng đầu</button>
          <button type="button" className="btn" onClick={() => updateRange(range.start, range.end - 1)} disabled={busy || range.end <= range.start}>Giảm cuối</button>
          <button type="button" className="btn" onClick={() => updateRange(range.start, range.end + 1)} disabled={busy || range.end >= totalPages}>Tăng cuối</button>
          <button type="button" className="btn btn-primary" onClick={onRefresh} disabled={busy}>Áp dụng split</button>
        </div>
      </div>
    </div>
  );
}

function PreviewCard({ title, subtitle, children }) {
  return (
    <div className="upload-auto-preview-card">
      <div className="upload-auto-preview-head">
        <div className="upload-auto-preview-title">{title}</div>
        <div className="upload-auto-preview-subtitle">{subtitle}</div>
      </div>
      {children}
    </div>
  );
}

function ProgressBox({ progress }) {
  return (
    <div style={{ marginTop: 16, border: "1px solid #dbeafe", borderRadius: 12, padding: 16, background: "#f8fbff" }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
        <strong>{progress.stageLabel}</strong>
        <strong>{Math.round(progress.percent || 0)}%</strong>
      </div>
      <div style={{ marginTop: 8 }}>{progress.message}</div>
      {progress.currentFileName ? <div style={{ marginTop: 6 }}>{progress.currentFileName}</div> : null}
      <div style={{ marginTop: 12, height: 10, borderRadius: 999, background: "#dbeafe", overflow: "hidden" }}>
        <div style={{ width: `${Math.max(0, Math.min(100, progress.percent || 0))}%`, height: "100%", background: "#2563eb" }} />
      </div>
    </div>
  );
}

function StatCard({ label, value }) {
  return (
    <div style={{ border: "1px solid #e2e8f0", borderRadius: 14, padding: 14, background: "#fff" }}>
      <div style={{ color: "#64748b", fontSize: 13 }}>{label}</div>
      <div style={{ marginTop: 6, fontSize: 24, fontWeight: 800, color: "#0f172a" }}>{value}</div>
    </div>
  );
}

function Field({ label, children }) {
  return (
    <div>
      <label style={labelStyle}>{label}</label>
      {children}
    </div>
  );
}

const labelStyle = {
  display: "block",
  marginBottom: 8,
  fontWeight: 700,
  color: "#334155",
};

const inputStyle = {
  width: "100%",
  border: "1px solid #dbe3ef",
  borderRadius: 12,
  padding: "12px 14px",
  fontSize: 14,
  outline: "none",
  background: "white",
};

const smallPillStyle = {
  display: "inline-flex",
  alignItems: "center",
  justifyContent: "center",
  borderRadius: 999,
  padding: "8px 14px",
  fontWeight: 800,
  minWidth: 78,
  fontSize: 14,
};

const toolbarInputStyle = {
  border: "1px solid #dbe3ef",
  borderRadius: 10,
  padding: "8px 10px",
  minWidth: 96,
  background: "#fff",
};

const iframePreviewStyle = {
  width: "100%",
  minHeight: 560,
  height: 560,
  border: "1px solid #dbe3ef",
  borderRadius: 16,
  background: "#fff",
};

const imgPreviewStyle = {
  width: "100%",
  minHeight: 560,
  objectFit: "contain",
  border: "1px solid #dbe3ef",
  borderRadius: 16,
  background: "#fff",
};

const emptyPreviewStyle = {
  border: "1px dashed #cbd5e1",
  borderRadius: 16,
  minHeight: 560,
  display: "grid",
  placeItems: "center",
  color: "#64748b",
  background: "#fff",
};

const errorStyle = {
  marginTop: 16,
  color: "#b91c1c",
  background: "#fef2f2",
  border: "1px solid #fecaca",
  borderRadius: 12,
  padding: 12,
};
