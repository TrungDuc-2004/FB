import { useEffect, useMemo, useState } from "react";
import "../styles/admin/modal.css";

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

function getClassNumber(path = "") {
  const m = String(path || "").match(/class-(\d+)/i);
  return m ? Number(m[1]) : 0;
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

function absPreview(url = "") {
  if (!url) return "";
  if (/^https?:\/\//i.test(url)) return url;
  return `${API_BASE}${url}`;
}

function badgeStyle(confidence = "high") {
  return confidence === "low"
    ? { background: "#fee2e2", color: "#b91c1c", border: "1px solid #fecaca" }
    : { background: "#dcfce7", color: "#166534", border: "1px solid #bbf7d0" };
}

function emptyItem() {
  return null;
}

function renderPreview(url = "", title = "preview") {
  const full = absPreview(url);
  if (!full) {
    return (
      <div
        style={{
          border: "1px dashed #cbd5e1",
          borderRadius: 10,
          minHeight: 240,
          display: "grid",
          placeItems: "center",
          background: "#fff",
        }}
      >
        Không có preview
      </div>
    );
  }

  const lower = String(full).toLowerCase();
  const imageExt = [".png", ".jpg", ".jpeg", ".webp"];
  const isImage = imageExt.some((ext) => lower.includes(ext)) || lower.includes("kind=top") || lower.includes("kind=bottom");

  if (isImage) {
    return (
      <img
        src={full}
        alt={title}
        style={{
          width: "100%",
          height: "100%",
          minHeight: 240,
          objectFit: "contain",
          border: "1px solid #e5e7eb",
          borderRadius: 10,
          background: "white",
        }}
      />
    );
  }

  return (
    <iframe
      title={title}
      src={full}
      style={{
        width: "100%",
        height: "100%",
        minHeight: 240,
        border: "1px solid #e5e7eb",
        borderRadius: 10,
        background: "white",
      }}
    />
  );
}

export default function UploadAutoModal({
  open,
  onClose,
  currentPath = "",
  onUploadAuto,
  onApproveAuto,
}) {
  const classNumber = getClassNumber(currentPath);
  const [bookVariant, setBookVariant] = useState(defaultVariantForClass(classNumber));
  const [file, setFile] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [progress, setProgress] = useState(null);
  const [phase, setPhase] = useState("upload");
  const [sessionData, setSessionData] = useState(null);
  const [items, setItems] = useState([]);
  const [selectedId, setSelectedId] = useState("");

  useEffect(() => {
    if (!open) {
      setFile(null);
      setBusy(false);
      setError("");
      setProgress(null);
      setBookVariant(defaultVariantForClass(classNumber));
      setPhase("upload");
      setSessionData(null);
      setItems([]);
      setSelectedId("");
      return;
    }

    if (classNumber === 10) {
      setBookVariant("TH10");
    }
  }, [open, classNumber]);

  const bookLabel = useMemo(() => {
    if (classNumber === 10) return "TH10";
    if (bookVariant === "UD") return `TH${classNumber}-UD`;
    if (bookVariant === "KHMT") return `TH${classNumber}-KHMT`;
    return "";
  }, [classNumber, bookVariant]);

  const selectedItem = useMemo(
    () => items.find((item) => item.reviewId === selectedId) || emptyItem(),
    [items, selectedId]
  );

  const topicOptions = useMemo(() => items.filter((x) => x.kind === "topic"), [items]);
  const lessonOptions = useMemo(() => items.filter((x) => x.kind === "lesson"), [items]);

  if (!open) return null;

  function patchItem(reviewId, patch) {
    setItems((prev) => prev.map((item) => (item.reviewId === reviewId ? { ...item, ...patch } : item)));
  }

  async function handleSubmit(e) {
    e?.preventDefault?.();

    if (!file) {
      setError("Hãy chọn file PDF.");
      return;
    }

    if (classNumber !== 10 && !bookVariant) {
      setError("Hãy chọn loại sách.");
      return;
    }

    setBusy(true);
    setError("");
    setProgress({
      stageLabel: "Đang gửi file lên server",
      message: "Đang gửi file lên server",
      percent: 0,
      totalFiles: 1,
      currentFileIndex: 0,
      currentFileName: file.name,
      status: "uploading",
    });

    try {
      const res = await onUploadAuto(file, {
        bookVariant: classNumber === 10 ? "" : bookVariant,
        onProgress: (payload) => setProgress(normalizeProgress(payload)),
      });
      if (res?.status !== "awaiting_review") {
        throw new Error("Backend chưa trả phiên duyệt. Hãy kiểm tra lại flow upload auto.");
      }
      const nextItems = Array.isArray(res.items) ? res.items : [];
      setSessionData(res);
      setItems(nextItems);
      setSelectedId(nextItems[0]?.reviewId || "");
      setPhase("review");
      setProgress(null);
    } catch (err) {
      setError(String(err?.message || err));
    } finally {
      setBusy(false);
    }
  }

  async function handleApprove() {
    if (!sessionData?.session_id) return;
    setBusy(true);
    setError("");
    setProgress({
      stageLabel: "Đang xác nhận dữ liệu",
      message: "Đang xác nhận dữ liệu",
      percent: 0,
      totalFiles: 1,
      currentFileIndex: 0,
      currentFileName: "",
      status: "processing",
    });
    try {
      await onApproveAuto(sessionData.session_id, items, {
        onProgress: (payload) => setProgress(normalizeProgress(payload)),
      });
      onClose?.(true);
    } catch (err) {
      setError(String(err?.message || err));
    } finally {
      setBusy(false);
    }
  }

  const lowCount = items.filter((x) => x.confidence === "low").length;
  const highCount = items.filter((x) => x.confidence === "high").length;

  const uploadStep = (
    <form className="modal-form" onSubmit={handleSubmit}>
      <div className="modal-body">
        <div className="field">
          <label>Đường dẫn</label>
          <input type="text" value={currentPath} readOnly />
        </div>

        <div className="field">
          <label>Sách</label>
          {classNumber === 10 ? (
            <input type="text" value={bookLabel || "TH10"} readOnly />
          ) : (
            <>
              <select
                value={bookVariant}
                onChange={(e) => setBookVariant(e.target.value)}
                disabled={busy}
              >
                <option value="">Chọn loại sách</option>
                <option value="UD">Ứng dụng (UD)</option>
                <option value="KHMT">Khoa học máy tính (KHMT)</option>
              </select>
              <input
                type="text"
                value={bookLabel}
                readOnly
                placeholder="Subject map sẽ tự suy ra"
                style={{ marginTop: 8 }}
              />
            </>
          )}
        </div>

        <div className="field">
          <label>Chọn file PDF</label>
          <input
            type="file"
            accept="application/pdf,.pdf"
            disabled={busy}
            onChange={(e) => {
              const picked = e.target.files?.[0] || null;
              setFile(picked);
              setError("");
            }}
          />
          {file && (
            <div className="file-info">
              <div>
                <strong>File:</strong> {file.name}
              </div>
              <div>
                <strong>Dung lượng:</strong> {formatKb(file.size)}
              </div>
            </div>
          )}
        </div>

        {progress && (
          <div className="modal-note" style={{ marginTop: 0 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
              <strong>{progress.stageLabel}</strong>
              <strong style={{ color: "#2563eb" }}>{Math.round(progress.percent || 0)}%</strong>
            </div>
            <div style={{ marginTop: 8 }}>{progress.message}</div>
            <div style={{ marginTop: 12, height: 10, background: "#dbeafe", borderRadius: 999, overflow: "hidden" }}>
              <div style={{ width: `${Math.max(0, Math.min(100, progress.percent || 0))}%`, height: "100%", background: "#3b82f6" }} />
            </div>
          </div>
        )}

        {error ? <div style={{ color: "#dc2626", whiteSpace: "pre-wrap", marginTop: 16 }}>{error}</div> : null}
      </div>

      <div className="modal-footer">
        <button type="button" className="btn" onClick={() => !busy && onClose?.()} disabled={busy}>
          Huỷ
        </button>
        <button type="submit" className="btn btn-primary" disabled={busy || !file}>
          {busy ? "Đang xử lý..." : "Tải lên và cắt tự động"}
        </button>
      </div>
    </form>
  );

  const reviewStep = (
    <div className="modal-form">
      <div className="modal-body" style={{ paddingBottom: 0 }}>
        <div className="modal-note" style={{ marginBottom: 16 }}>
          Hệ thống đã cắt xong. Mục màu xanh là độ tin cậy cao, màu đỏ là độ tin cậy thấp.
          Nếu thấy sai, bạn sửa trực tiếp rồi bấm duyệt để hệ thống mới bắt đầu sinh description,
          keyword cho chunk và đẩy keyword từ chunk lên lesson, topic, subject.
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "260px minmax(320px, 420px) minmax(0, 1fr)", gap: 16, minHeight: 520 }}>
          <div style={{ border: "1px solid #dbe2ea", borderRadius: 12, overflow: "hidden", display: "flex", flexDirection: "column", minHeight: 0 }}>
            <div style={{ padding: 12, borderBottom: "1px solid #e5e7eb", background: "#f8fafc" }}>
              <div><strong>Tổng mục:</strong> {items.length}</div>
              <div><strong>Độ tin cậy cao:</strong> {highCount}</div>
              <div><strong>Độ tin cậy thấp:</strong> {lowCount}</div>
            </div>
            <div style={{ overflowY: "auto", padding: 8, display: "flex", flexDirection: "column", gap: 8, minHeight: 0 }}>
              {items.map((item) => (
                <button
                  key={item.reviewId}
                  type="button"
                  onClick={() => setSelectedId(item.reviewId)}
                  style={{
                    textAlign: "left",
                    borderRadius: 10,
                    padding: 10,
                    border: item.reviewId === selectedId ? "2px solid #2563eb" : "1px solid #dbe2ea",
                    background: "white",
                    cursor: "pointer",
                  }}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "flex-start" }}>
                    <strong style={{ textTransform: "capitalize" }}>{item.kind}</strong>
                    <span style={{ ...badgeStyle(item.confidence), borderRadius: 999, padding: "2px 8px", fontSize: 12 }}>
                      {item.confidence === "low" ? "Độ tin cậy thấp" : "Độ tin cậy cao"}
                    </span>
                  </div>
                  <div style={{ marginTop: 6, fontSize: 13 }}>{item.heading || item.title || item.name}</div>
                  <div style={{ marginTop: 4, fontSize: 12, color: "#64748b" }}>
                    Trang {item.start} - {item.end}
                  </div>
                </button>
              ))}
            </div>
          </div>

          <div style={{ border: "1px solid #dbe2ea", borderRadius: 12, padding: 16, overflowY: "auto", minHeight: 0 }}>
            {selectedItem ? (
              <>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, marginBottom: 12 }}>
                  <h4 style={{ margin: 0, textTransform: "capitalize" }}>{selectedItem.kind}</h4>
                  <span style={{ ...badgeStyle(selectedItem.confidence), borderRadius: 999, padding: "4px 10px", fontSize: 12 }}>
                    {selectedItem.confidence === "low" ? "Độ tin cậy thấp" : "Độ tin cậy cao"}
                  </span>
                </div>

                {selectedItem.confidenceReason ? (
                  <div style={{ marginBottom: 12, color: "#b45309", background: "#fff7ed", border: "1px solid #fed7aa", padding: 10, borderRadius: 10 }}>
                    {selectedItem.confidenceReason}
                  </div>
                ) : null}

                <div className="field">
                  <label>Heading</label>
                  <input value={selectedItem.heading || ""} onChange={(e) => patchItem(selectedItem.reviewId, { heading: e.target.value })} disabled={busy} />
                </div>
                <div className="field">
                  <label>Title</label>
                  <input value={selectedItem.title || ""} onChange={(e) => patchItem(selectedItem.reviewId, { title: e.target.value })} disabled={busy} />
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                  <div className="field">
                    <label>Trang bắt đầu</label>
                    <input type="number" value={selectedItem.start || 1} onChange={(e) => patchItem(selectedItem.reviewId, { start: Number(e.target.value || 1) })} disabled={busy} />
                  </div>
                  <div className="field">
                    <label>Trang kết thúc</label>
                    <input type="number" value={selectedItem.end || 1} onChange={(e) => patchItem(selectedItem.reviewId, { end: Number(e.target.value || 1) })} disabled={busy} />
                  </div>
                </div>

                {selectedItem.kind === "chunk" ? (
                  <>
                    <div className="field">
                      <label>Content head</label>
                      <select value={selectedItem.contentHead ? "1" : "0"} onChange={(e) => patchItem(selectedItem.reviewId, { contentHead: e.target.value === "1" })} disabled={busy}>
                        <option value="0">false</option>
                        <option value="1">true</option>
                      </select>
                    </div>
                    <div className="field">
                      <label>y_line cắt tay</label>
                      <input
                        type="number"
                        value={selectedItem.yLine ?? ""}
                        placeholder="Để trống nếu dùng tự động"
                        onChange={(e) => patchItem(selectedItem.reviewId, { yLine: e.target.value === "" ? null : Number(e.target.value) })}
                        disabled={busy}
                      />
                    </div>
                    <div className="field">
                      <label>Thuộc lesson</label>
                      <select
                        value={selectedItem.lessonReviewId || ""}
                        onChange={(e) => patchItem(selectedItem.reviewId, { lessonReviewId: e.target.value })}
                        disabled={busy}
                      >
                        <option value="">Chọn lesson</option>
                        {lessonOptions.map((opt) => (
                          <option key={opt.reviewId} value={opt.reviewId}>
                            {opt.heading || opt.title || opt.name}
                          </option>
                        ))}
                      </select>
                    </div>
                  </>
                ) : null}

                {selectedItem.kind === "lesson" ? (
                  <div className="field">
                    <label>Thuộc topic</label>
                    <select
                      value={selectedItem.topicReviewId || ""}
                      onChange={(e) => patchItem(selectedItem.reviewId, { topicReviewId: e.target.value })}
                      disabled={busy}
                    >
                      <option value="">Chọn topic</option>
                      {topicOptions.map((opt) => (
                        <option key={opt.reviewId} value={opt.reviewId}>
                          {opt.heading || opt.title || opt.name}
                        </option>
                      ))}
                    </select>
                  </div>
                ) : null}
              </>
            ) : (
              <div>Chọn một mục để xem và chỉnh.</div>
            )}
          </div>

          <div style={{ border: "1px solid #dbe2ea", borderRadius: 12, padding: 12, display: "grid", gridTemplateRows: "1fr 1fr", gap: 12, minHeight: 0 }}>
            <div style={{ minHeight: 0 }}>
              <div style={{ marginBottom: 8, fontWeight: 600 }}>Preview ngữ cảnh cha</div>
              {renderPreview(selectedItem?.previewContextUrl, "preview-context")}
            </div>
            <div style={{ minHeight: 0 }}>
              <div style={{ marginBottom: 8, fontWeight: 600 }}>Preview phần cắt hiện tại</div>
              {renderPreview(selectedItem?.previewCurrentUrl, "preview-current")}
            </div>
          </div>
        </div>

        {progress ? (
          <div className="modal-note" style={{ marginTop: 16 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
              <strong>{progress.stageLabel}</strong>
              <strong style={{ color: "#2563eb" }}>{Math.round(progress.percent || 0)}%</strong>
            </div>
            <div style={{ marginTop: 8 }}>{progress.message}</div>
            <div style={{ marginTop: 12, height: 10, background: "#dbeafe", borderRadius: 999, overflow: "hidden" }}>
              <div style={{ width: `${Math.max(0, Math.min(100, progress.percent || 0))}%`, height: "100%", background: "#3b82f6" }} />
            </div>
          </div>
        ) : null}

        {error ? <div style={{ color: "#dc2626", whiteSpace: "pre-wrap", marginTop: 16 }}>{error}</div> : null}
      </div>

      <div className="modal-footer">
        <button type="button" className="btn" disabled={busy} onClick={() => setPhase("upload")}>
          Quay lại
        </button>
        <button type="button" className="btn btn-primary" disabled={busy || !sessionData?.session_id} onClick={handleApprove}>
          {busy ? "Đang sync..." : "Duyệt và bắt đầu sync"}
        </button>
      </div>
    </div>
  );

  return (
    <div className="modal-overlay" onClick={() => !busy && onClose?.()}>
      <div
        className={`modal ${phase === "review" ? "modal-wide-review" : ""}`}
        style={phase === "review" ? { width: "96vw", maxWidth: "1400px" } : undefined}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="modal-header">
          <h3 className="modal-title">{phase === "upload" ? "Tải lên tự động" : "Duyệt kết quả cắt tự động"}</h3>
          <button type="button" className="modal-close" onClick={() => !busy && onClose?.()} disabled={busy} aria-label="Đóng">
            ×
          </button>
        </div>
        {phase === "upload" ? uploadStep : reviewStep}
      </div>
    </div>
  );
}
