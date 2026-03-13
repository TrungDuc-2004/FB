import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
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
    } catch (error) {
      setErr(String(error?.message || error));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  async function onToggleSave(doc) {
    try {
      const response = await toggleSave(doc.chunkID, doc?.category || "document");
      if (!response.saved) {
        setItems((prev) =>
          (prev || []).filter(
            (item) => !(item.chunkID === doc.chunkID && (item.category || "document") === (doc.category || "document"))
          )
        );
        return;
      }

      setItems((prev) =>
        (prev || []).map((item) =>
          item.chunkID === doc.chunkID && (item.category || "document") === (doc.category || "document")
            ? { ...item, isSaved: true }
            : item
        )
      );
    } catch (error) {
      alert(String(error?.message || error));
    }
  }

  return (
    <div className="search-page-shell">
      <div className="search-results-header">
        <div>
          <div className="search-results-title">Tài liệu đã lưu</div>
          <div className="search-results-subtitle">{items.length} mục đang được lưu</div>
        </div>

        <div className="saved-page-actions">
          <button className="saved-page-button" type="button" onClick={load} disabled={loading}>
            Tải lại
          </button>
          <button className="saved-page-button" type="button" onClick={() => navigate("/user/search")}>
            Tìm tài liệu khác
          </button>
        </div>
      </div>

      {err ? <div className="search-inline-message danger">Lỗi: {err}</div> : null}
      {loading && items.length === 0 ? <div className="search-empty-state">Đang tải...</div> : null}
      {!loading && items.length === 0 ? <div className="search-empty-state">Chưa có tài liệu nào được lưu.</div> : null}

      {!loading && items.length > 0 ? (
        <div className="search-results-list">
          {(items || []).map((doc) => (
            <DocumentCard key={`${doc.chunkID}-${doc.category || "document"}`} doc={doc} onToggleSave={onToggleSave} variant="search-list" />
          ))}
        </div>
      ) : null}
    </div>
  );
}
