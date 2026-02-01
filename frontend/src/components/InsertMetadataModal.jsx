// components/InsertMetadataModel.jsx
import { useState } from "react";
import "../styles/admin/modal.css";

export default function InsertMetadataModal({ open, onClose, folderName, onInsert }) {
  const [meta, setMeta] = useState({
    class: "",
    subject: "",
    topic: "",
    lesson: "",
    chunk: "",
    name: "",
  });
  const [file, setFile] = useState(null);

  if (!open) return null;

  function change(k, v) {
    setMeta((prev) => ({ ...prev, [k]: v }));
  }

  function submit(e) {
    e.preventDefault();
    if (!meta.name.trim() && !file) {
      alert("Vui lòng nhập tên hoặc chọn file");
      return;
    }
    onInsert({ meta, file });
    setMeta({ class: "", subject: "", topic: "", lesson: "", chunk: "", name: "" });
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
            <div className="form-grid">
              <div className="field">
                <label htmlFor="class">Lớp học</label>
                <input
                  id="class"
                  value={meta.class}
                  onChange={(e) => change("class", e.target.value)}
                  placeholder="10A1, 11B2, ..."
                />
              </div>
              <div className="field">
                <label htmlFor="subject">Môn học</label>
                <input
                  id="subject"
                  value={meta.subject}
                  onChange={(e) => change("subject", e.target.value)}
                  placeholder="Toán, Lý, Hoá, ..."
                />
              </div>
              <div className="field">
                <label htmlFor="topic">Chủ đề</label>
                <input
                  id="topic"
                  value={meta.topic}
                  onChange={(e) => change("topic", e.target.value)}
                  placeholder="Hàm số, Cơ học, ..."
                />
              </div>
              <div className="field">
                <label htmlFor="lesson">Bài học</label>
                <input
                  id="lesson"
                  value={meta.lesson}
                  onChange={(e) => change("lesson", e.target.value)}
                  placeholder="Bài 1, Chương 2, ..."
                />
              </div>
              <div className="field">
                <label htmlFor="chunk">Chunk</label>
                <input
                  id="chunk"
                  value={meta.chunk}
                  onChange={(e) => change("chunk", e.target.value)}
                  placeholder="chunk-001, part-01, ..."
                />
              </div>
              <div className="field">
                <label htmlFor="name">Tên file</label>
                <input
                  id="name"
                  value={meta.name}
                  onChange={(e) => change("name", e.target.value)}
                  placeholder="bai1.pdf, video1.mp4, ..."
                  required
                />
              </div>
            </div>

            <div className="field">
              <label htmlFor="file">Chọn file (tùy chọn)</label>
              <input id="file" type="file" onChange={(e) => setFile(e.target.files?.[0] || null)} />
              {file && (
                <div className="file-info">
                  <strong>Đã chọn:</strong> {file.name} ({Math.round(file.size / 1024)} KB)
                </div>
              )}
            </div>

            <div className="modal-note">
              <strong>Lưu ý:</strong> Metadata này chỉ dùng để demo UI. Sau này sẽ tích hợp API để
              lưu metadata và upload file thực tế.
            </div>
          </form>
        </div>

        <div className="modal-footer">
          <button className="btn" onClick={onClose}>
            Huỷ
          </button>
          <button className="btn btn-primary" onClick={submit}>
            Thêm metadata
          </button>
        </div>
      </div>
    </div>
  );
}
