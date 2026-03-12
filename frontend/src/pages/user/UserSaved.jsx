import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import "../../styles/admin/page.css";
import DocumentCard from "../../components/DocumentCard";
import { listSaved, toggleSave } from "../../services/userDocsApi";

export default function UserSaved() {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [items, setItems] = useState([]);

  async function load() {
    setLoading(true);
    setErr("");
    try {
      const data = await listSaved({ category: "all", limit: 100, offset: 0 });
      setItems(data.items || []);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  async function onToggleSave(doc) {
    try {
      const r = await toggleSave(doc.chunkID, doc?.category || "document");
      if (!r.saved) {
        setItems((prev) => (prev || []).filter((item) => !(item.chunkID === doc.chunkID && (item.category || "document") === (doc.category || "document"))));
        return;
      }
      setItems((prev) => (prev || []).map((item) => (item.chunkID === doc.chunkID && (item.category || "document") === (doc.category || "document") ? { ...item, isSaved: true } : item)));
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div>
              <div className="page-title">Tài liệu đã lưu</div>
              <div className="breadcrumb">
                <span className="crumb">User</span>
                <span className="crumb">Đã lưu</span>
                <span className="crumb">{items.length} tài liệu</span>
              </div>
            </div>
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1.5fr) minmax(280px, 1fr)", gap: 16, alignItems: "stretch" }}>
          <div style={{ border: "1px solid #fcd34d", borderRadius: 18, padding: 18, background: "linear-gradient(180deg, #fffdf5 0%, #ffffff 100%)" }}>
            <div style={{ fontSize: 13, fontWeight: 800, color: "#b45309", marginBottom: 8 }}>KHU VỰC XEM LẠI</div>
            <div style={{ fontSize: 24, fontWeight: 800, color: "#0f172a", marginBottom: 8 }}>Những tài liệu quan trọng bạn đã đánh dấu sẽ nằm ở đây.</div>
            <div style={{ color: "#475569", lineHeight: 1.7 }}>
              Bạn có thể mở lại chi tiết, xem trực tiếp tài liệu hoặc bấm bỏ lưu ngay trên từng thẻ để dọn danh sách.
            </div>
          </div>

          <div style={{ border: "1px solid #e2e8f0", borderRadius: 18, padding: 18, background: "#fff", display: "grid", gap: 12 }}>
            <div>
              <div style={{ fontSize: 13, color: "#475569", fontWeight: 700, marginBottom: 6 }}>Tổng quan</div>
              <div style={{ fontSize: 30, fontWeight: 800, color: "#0f172a" }}>{items.length}</div>
              <div style={{ color: "#64748b" }}>mục đang được lưu</div>
            </div>
            <div className="header-actions">
              <button className="btn" type="button" onClick={load} disabled={loading}>
                Tải lại
              </button>
              <button className="btn" type="button" onClick={() => navigate("/user/library")}>
                Mở thư viện
              </button>
              <button className="btn btn-primary" type="button" onClick={() => navigate("/user/search")}>
                Tìm tài liệu khác
              </button>
            </div>
          </div>
        </div>
      </div>

      {err ? <div style={{ color: "#b91c1c", fontWeight: 700 }}>Lỗi: {err}</div> : null}
      {loading && items.length === 0 ? <div className="empty-state">Đang tải...</div> : null}
      {!loading && items.length === 0 ? <div className="empty-state">Chưa có tài liệu nào được lưu.</div> : null}

      <div style={{ display: "grid", gap: 12 }}>
        {(items || []).map((doc) => (
          <DocumentCard key={`${doc.chunkID}-${doc.category || "document"}`} doc={doc} onToggleSave={onToggleSave} />
        ))}
      </div>
    </div>
  );
}
