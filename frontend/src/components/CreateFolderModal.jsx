// components/CreateFolderModal.jsx
import { useState } from "react";
import "../styles/admin/modal.css";

export default function CreateFolderModal({ open, onClose, onCreate }) {
  const [name, setName] = useState("");

  if (!open) return null;

  function submit(e) {
    e.preventDefault();
    const n = name.trim();
    if (!n) return;
    onCreate(n);
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
          <form onSubmit={submit}>
            <div className="field">
              <label htmlFor="folder-name">Tên folder</label>
              <input
                id="folder-name"
                type="text"
                placeholder="Ví dụ: documents, images, reports"
                value={name}
                onChange={(e) => setName(e.target.value)}
                autoFocus
              />
            </div>

            <div className="modal-note">
              <strong>Lưu ý:</strong> Tên folder không được trùng với folder đã tồn tại.
              <br />
              (Demo UI) Sau này sẽ gọi API tạo folder thật trên MinIO.
            </div>
          </form>
        </div>

        <div className="modal-footer">
          <button className="btn" onClick={onClose}>
            Huỷ
          </button>
          <button className="btn btn-primary" onClick={submit}>
            Tạo folder
          </button>
        </div>
      </div>
    </div>
  );
}
