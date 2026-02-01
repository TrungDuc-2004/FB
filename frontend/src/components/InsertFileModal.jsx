// components/InsertFileModel.jsx
import { useState } from "react";
import "../styles/admin/modal.css";

export default function InsertFileModal({ open, onClose, folderName, onInsert }) {
  const [name, setName] = useState("");

  if (!open) return null;

  function submit() {
    const n = name.trim();
    if (!n) return;
    onInsert(n);
    setName("");
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="modal-title">Tạo thư mục mới</h3>
          <p className="modal-subtitle">Thêm thư mục để tổ chức tập tin</p>
          <button className="modal-close" onClick={onClose}>
            ×
          </button>
        </div>

        <div className="modal-body">
          <div className="field">
            <label>Tên file (insert)</label>
            <input
              placeholder="vd: note.txt"
              value={name}
              onChange={(e) => setName(e.target.value)}
              autoFocus
            />
          </div>

          <div style={{ fontSize: 12, color: "rgba(15,23,42,0.6)" }}>
            Insert = thêm 1 dòng file “giả” để demo UI. Sau này bạn đổi thành insert metadata / tạo
            object kiểu khác.
          </div>
        </div>

        <div className="modal-footer">
          <button className="btn" onClick={onClose}>
            Huỷ
          </button>
          <button className="btn btn-primary" onClick={submit}>
            Insert
          </button>
        </div>
      </div>
    </div>
  );
}
