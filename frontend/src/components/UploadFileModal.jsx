// components/UploadFileModal.jsx
import { useState } from "react";
import "../styles/admin/modal.css";

export default function UploadFileModal({ open, onClose, folderName, onUpload }) {
  const [file, setFile] = useState(null);

  if (!open) return null;

  function submit(e) {
    e.preventDefault();
    if (!file) return;
    onUpload(file);
    setFile(null);
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
              <label htmlFor="upload-file">Chọn file để upload</label>
              <input
                id="upload-file"
                type="file"
                onChange={(e) => setFile(e.target.files?.[0] || null)}
              />
              {file && (
                <div className="file-info">
                  <div>
                    <strong>Tên file:</strong> {file.name}
                  </div>
                  <div>
                    <strong>Kích thước:</strong> {Math.round(file.size / 1024)} KB
                  </div>
                  <div>
                    <strong>Loại file:</strong> {file.type || "Không xác định"}
                  </div>
                </div>
              )}
            </div>

            <div className="modal-note">
              <strong>Lưu ý:</strong> Demo UI - File chỉ được thêm vào bảng dữ liệu. Sau này sẽ tích
              hợp API upload thực tế lên MinIO.
            </div>
          </form>
        </div>

        <div className="modal-footer">
          <button className="btn" onClick={onClose}>
            Huỷ
          </button>
          <button className="btn btn-primary" onClick={submit} disabled={!file}>
            Upload file
          </button>
        </div>
      </div>
    </div>
  );
}
