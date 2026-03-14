import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import DocumentCard from "../../components/DocumentCard";
import {
  clearSearchHistory,
  clearViewHistory,
  listSearchHistory,
  listViewHistory,
  removeSearchHistory,
  removeViewHistory,
  toggleSave,
} from "../../services/userDocsApi";
import "../../styles/user/search.css";

function formatViewedAt(value) {
  if (!value) return "Vừa xem";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Vừa xem";
  return new Intl.DateTimeFormat("vi-VN", {
    hour: "2-digit",
    minute: "2-digit",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  }).format(date);
}

export default function UserHistory() {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [viewItems, setViewItems] = useState([]);
  const [searchItems, setSearchItems] = useState([]);

  async function load() {
    try {
      setLoading(true);
      setError("");
      const [viewsRes, searchRes] = await Promise.all([
        listViewHistory({ category: "all", limit: 50, offset: 0 }),
        listSearchHistory({ limit: 200 }),
      ]);
      setViewItems(Array.isArray(viewsRes?.items) ? viewsRes.items : []);
      setSearchItems(Array.isArray(searchRes?.items) ? searchRes.items : []);
    } catch (err) {
      setError(String(err?.message || err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  async function onToggleSave(doc) {
    try {
      if (!doc?.chunkID) return;
      const res = await toggleSave(doc.chunkID, doc?.category || "document");
      setViewItems((prev) =>
        (prev || []).map((item) =>
          item.chunkID === doc.chunkID && (item.category || "document") === (doc.category || "document")
            ? { ...item, isSaved: res.saved }
            : item
        )
      );
    } catch (err) {
      setError(String(err?.message || err));
    }
  }

  async function onRemoveHistory(doc) {
    try {
      if (!doc?.chunkID) return;
      await removeViewHistory(doc.chunkID, { category: doc?.category || "document" });
      setViewItems((prev) =>
        (prev || []).filter(
          (item) => !(item.chunkID === doc.chunkID && (item.category || "document") === (doc.category || "document"))
        )
      );
    } catch (err) {
      setError(String(err?.message || err));
    }
  }

  async function onClearViewHistory() {
    try {
      await clearViewHistory({ category: "all" });
      setViewItems([]);
    } catch (err) {
      setError(String(err?.message || err));
    }
  }

  async function onRemoveSearch(keyword) {
    try {
      await removeSearchHistory(keyword);
      setSearchItems((prev) => (prev || []).filter((item) => item.keyword !== keyword));
    } catch (err) {
      setError(String(err?.message || err));
    }
  }

  async function onClearSearchHistory() {
    try {
      await clearSearchHistory();
      setSearchItems([]);
    } catch (err) {
      setError(String(err?.message || err));
    }
  }

  function onClickKeyword(keyword) {
    const text = String(keyword || "").trim();
    if (!text) return;
    navigate(`/user/search?q=${encodeURIComponent(text)}`);
  }

  return (
    <div className="search-page-shell">
      <div className="search-results-header">
        <div>
          <div className="search-results-title">Lịch sử xem tài liệu</div>
          <div className="search-results-subtitle">
            {viewItems.length} mục đã xem gần đây · {searchItems.length} từ khóa trong lịch sử tìm kiếm
          </div>
        </div>

        <div className="saved-page-actions">
          <button className="saved-page-button" type="button" onClick={load} disabled={loading}>
            Tải lại
          </button>
          <button className="saved-page-button" type="button" onClick={onClearSearchHistory} disabled={loading || !searchItems.length}>
            Xóa lịch sử tìm kiếm
          </button>
          <button className="saved-page-button" type="button" onClick={onClearViewHistory} disabled={loading || !viewItems.length}>
            Xóa lịch sử xem
          </button>
        </div>
      </div>

      {error ? <div className="search-inline-message danger">Lỗi: {error}</div> : null}

      <section className="history-search-card">
        <div className="history-section-head">
          <div>
            <div className="history-section-title">Toàn bộ lịch sử tìm kiếm</div>
            <div className="search-results-subtitle">Header search chỉ hiện 5 mục gần nhất, còn ở đây hiển thị toàn bộ theo từng tài khoản.</div>
          </div>
        </div>

        {searchItems.length ? (
          <div className="history-search-chip-list">
            {searchItems.map((item) => (
              <div key={item.keyword} className="history-search-chip">
                <button type="button" className="main" onClick={() => onClickKeyword(item.keyword)}>
                  {item.keyword}
                </button>
                <button type="button" className="remove" onClick={() => onRemoveSearch(item.keyword)} aria-label={`Xóa ${item.keyword}`}>
                  ×
                </button>
              </div>
            ))}
          </div>
        ) : (
          <div className="search-empty-state">Chưa có lịch sử tìm kiếm cho tài khoản này.</div>
        )}
      </section>

      <section className="history-view-section">
        <div className="history-section-head">
          <div className="history-section-title">Tài liệu đã xem gần đây</div>
          <div className="search-results-subtitle">Danh sách này cũng tách riêng theo từng tài khoản.</div>
        </div>

        {loading && !viewItems.length ? <div className="search-empty-state">Đang tải lịch sử xem...</div> : null}
        {!loading && !viewItems.length ? <div className="search-empty-state">Chưa có tài liệu nào trong lịch sử xem.</div> : null}

        {viewItems.length ? (
          <div className="search-results-list">
            {viewItems.map((doc) => (
              <div key={`${doc.chunkID}-${doc.category || "document"}`} className="history-doc-row">
                <div className="history-doc-row-head">
                  <span>{formatViewedAt(doc.historyViewedAt)}</span>
                  <button type="button" onClick={() => onRemoveHistory(doc)}>
                    Xóa khỏi lịch sử
                  </button>
                </div>
                <DocumentCard doc={doc} onToggleSave={onToggleSave} variant="search-list" />
              </div>
            ))}
          </div>
        ) : null}
      </section>
    </div>
  );
}
