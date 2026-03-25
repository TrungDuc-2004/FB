import { useEffect, useMemo, useState } from "react";
import "../styles/admin/modal.css";

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

export default function UploadAutoModal({ open, onClose, currentPath = "", onUploadAuto }) {
  const classNumber = getClassNumber(currentPath);
  const [bookVariant, setBookVariant] = useState(defaultVariantForClass(classNumber));
  const [file, setFile] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [progress, setProgress] = useState(null);

  useEffect(() => {
    if (!open) {
      setFile(null);
      setBusy(false);
      setError("");
      setProgress(null);
      setBookVariant(defaultVariantForClass(classNumber));
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

  if (!open) return null;

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
      await onUploadAuto(file, {
        bookVariant: classNumber === 10 ? "" : bookVariant,
        onProgress: (payload) => setProgress(normalizeProgress(payload)),
      });
      onClose?.();
    } catch (err) {
      setError(String(err?.message || err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="modal-overlay" onClick={() => !busy && onClose?.()}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="modal-title">Upload auto</h3>
          <p className="modal-subtitle">
            Tự cắt PDF theo cấu trúc học liệu rồi sync vào hệ thống
          </p>
          <button
            type="button"
            className="modal-close"
            onClick={() => !busy && onClose?.()}
            disabled={busy}
            aria-label="Đóng"
          >
            ×
          </button>
        </div>

        <form className="modal-form" onSubmit={handleSubmit}>
          <div className="modal-body">
            <div className="field">
              <label>Thư mục hiện tại</label>
              <input type="text" value={currentPath} readOnly />
            </div>

            <div className="field">
              <label>Loại sách</label>
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
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    gap: 12,
                  }}
                >
                  <strong>{progress.stageLabel}</strong>
                  <strong style={{ color: "#2563eb" }}>
                    {Math.round(progress.percent || 0)}%
                  </strong>
                </div>

                <div style={{ marginTop: 8 }}>{progress.message}</div>
                {progress.currentFileName ? (
                  <div style={{ marginTop: 4, wordBreak: "break-word" }}>
                    {progress.currentFileName}
                  </div>
                ) : null}

                <div
                  style={{
                    marginTop: 12,
                    height: 10,
                    background: "#dbeafe",
                    borderRadius: 999,
                    overflow: "hidden",
                  }}
                >
                  <div
                    style={{
                      width: `${Math.max(0, Math.min(100, progress.percent || 0))}%`,
                      height: "100%",
                      background: "#3b82f6",
                    }}
                  />
                </div>

                <div
                  style={{
                    marginTop: 10,
                    display: "flex",
                    justifyContent: "space-between",
                    gap: 12,
                    fontSize: 13,
                  }}
                >
                  <span>Status: {progress.status}</span>
                  <span>
                    File {progress.currentFileIndex || 0}/{progress.totalFiles || 1}
                  </span>
                </div>
              </div>
            )}

            <div className="modal-note">
              <strong>Lưu ý:</strong> Hệ thống sẽ tự suy ra ID theo cây subject → topic → lesson.
              Upload ở subjects thì hệ thống cắt sách gốc; upload ở topics thì hệ thống cắt tiếp xuống lesson.
              Hiện tại dừng ở lesson, chưa cắt chunk.
            </div>

            {error ? (
              <div style={{ color: "#dc2626", whiteSpace: "pre-wrap", marginTop: 16 }}>
                {error}
              </div>
            ) : null}
          </div>

          <div className="modal-footer">
            <button type="button" className="btn" onClick={() => !busy && onClose?.()} disabled={busy}>
              Huỷ
            </button>
            <button type="submit" className="btn btn-primary" disabled={busy || !file}>
              {busy ? "Đang xử lý..." : "Upload auto"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
