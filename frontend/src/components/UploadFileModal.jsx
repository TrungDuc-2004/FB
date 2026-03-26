import { useState } from "react";
import "../styles/admin/modal.css";

export default function UploadFileModal({ open, onClose, folderName, onUpload }) {
  const [files, setFiles] = useState([]);

  if (!open) return null;

  function reset() {
    setFiles([]);
  }

  function submit(e) {
    if (e?.preventDefault) e.preventDefault();
    if (!files.length) return;
    onUpload(files);
    reset();
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="modal-title">Tải file lên</h3>
          
          <button
            className="modal-close"
            onClick={() => {
              reset();
              onClose();
            }}
          >
            ×
          </button>
        </div>

        <div className="modal-body">
          <form onSubmit={submit}>
            <div className="field">
              <label htmlFor="upload-folder">Đường dẫn</label>
              <input id="upload-folder" type="text" value={folderName || ""} disabled />
            </div>

            <div className="field">
              <label htmlFor="upload-file">Chọn file để upload</label>
              <input
                id="upload-file"
                type="file"
                multiple
                onChange={(e) => setFiles(Array.from(e.target.files || []))}
              />

              {files.length > 0 && (
                <div className="file-info">
                  <div>
                    <strong>Số file:</strong> {files.length}
                  </div>
                  <div>
                    <strong>Tổng dung lượng:</strong>{" "}
                    {Math.round(files.reduce((sum, f) => sum + (f?.size || 0), 0) / 1024)} KB
                  </div>
                  <div style={{ maxHeight: 180, overflow: "auto", marginTop: 8 }}>
                    {files.map((f) => (
                      <div key={`${f.name}-${f.size}`}>
                        • {f.name} ({Math.round((f.size || 0) / 1024)} KB)
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>

            <div className="modal-note">
              <strong>Lưu ý:</strong> Sau khi upload xong, hệ thống sẽ báo rõ file nào thành công và file nào lỗi.
            </div>
          </form>
        </div>

        <div className="modal-footer">
          <button
            className="btn"
            onClick={() => {
              reset();
              onClose();
            }}
          >
            Huỷ
          </button>
          <button className="btn btn-primary" onClick={submit} disabled={!files.length}>
            Tải file lên
          </button>
        </div>
      </div>
    </div>
  );
}
