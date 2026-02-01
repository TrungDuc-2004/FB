import { useEffect, useState } from "react";

export default function FilterModal({
  open,
  onClose,
  initialValue,
  onApply,
  availableTypes = [],
}) {
  const [loai, setLoai] = useState("all");
  const [type, setType] = useState("all");

  useEffect(() => {
    if (!open) return;
    setLoai(initialValue?.loai ?? "all");
    setType(initialValue?.type ?? "all");
  }, [open, initialValue]);

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
            <select value={loai} onChange={(e) => setLoai(e.target.value)}>
              <option value="all">Tất cả loại</option>
              <option value="document">Document</option>
              <option value="image">Image</option>
              <option value="video">Video</option>
            </select>
          </div>

          <div className="form-row">
            <label>Type (đuôi file)</label>
            <select value={type} onChange={(e) => setType(e.target.value)}>
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
              setLoai("all");
              setType("all");
            }}
          >
            Reset bộ lọc
          </button>

          <button
            className="btn btn-primary"
            onClick={() => {
              onApply({ loai, type });
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
