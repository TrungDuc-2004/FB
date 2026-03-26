import { useState } from "react";

export default function FilterModal({
  open,
  onClose,
  initialValue,
  onApply,
  availableTypes = [],
}) {
  const [draft, setDraft] = useState(() => ({
    loai: initialValue?.loai ?? "all",
    type: initialValue?.type ?? "all",
  }));

  if (!open) return null;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Bộ lọc</h3>
          <p>Lọc theo loại (document/image/video) và theo type (đuôi file)</p>
        </div>

        <div className="modal-body">
          <div className="form-row">
            <label>Loại</label>
            <select
              value={draft.loai}
              onChange={(e) => setDraft((prev) => ({ ...prev, loai: e.target.value }))}
            >
              <option value="all">Tất cả loại</option>
              <option value="document">Document</option>
              <option value="image">Image</option>
              <option value="video">Video</option>
            </select>
          </div>

          <div className="form-row">
            <label>Type (đuôi file)</label>
            <select
              value={draft.type}
              onChange={(e) => setDraft((prev) => ({ ...prev, type: e.target.value }))}
            >
              <option value="all">Tất cả type</option>
              {availableTypes.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>
        </div>

        <div className="modal-footer">
          <button
            className="btn"
            onClick={() => {
              setDraft({ loai: "all", type: "all" });
            }}
          >
            Reset bộ lọc
          </button>

          <button
            className="btn btn-primary"
            onClick={() => {
              onApply(draft);
              onClose();
            }}
          >
            Áp dụng
          </button>
        </div>
      </div>
    </div>
  );
}
