import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import "../../styles/admin/page.css";
import { getDocDetail, getDocViewUrl, toggleSave } from "../../services/userDocsApi";

function getExt(url) {
  const u = (url || "").split("?")[0];
  const parts = u.split(".");
  return (parts[parts.length - 1] || "").toLowerCase();
}

export default function DocumentView() {
  const { chunkID } = useParams();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [doc, setDoc] = useState(null);
  const [view, setView] = useState(null);

  async function load() {
    try {
      setLoading(true);
      setErr("");

      const [detail, viewRes] = await Promise.all([
        getDocDetail(chunkID),
        getDocViewUrl(chunkID).catch(() => null),
      ]);

      setDoc(detail);
      setView(viewRes);
    } catch (e) {
      setErr(String(e?.message || e));
      setDoc(null);
      setView(null);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, [chunkID]);

  async function onToggleSave() {
    try {
      const r = await toggleSave(chunkID);
      setDoc((prev) => (prev ? { ...prev, isSaved: r.saved } : prev));
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  const originalUrl = doc?.chunkUrl || "";
  const viewUrl = view?.viewUrl || originalUrl;
  const ext = useMemo(() => getExt(viewUrl || originalUrl), [viewUrl, originalUrl]);
  const title = doc?.chunkName || chunkID;

  const canIframe = ["pdf"].includes(ext);
  const canImage = ["png", "jpg", "jpeg", "webp", "gif"].includes(ext);
  const canVideo = ["mp4", "webm", "ogg"].includes(ext);

  return (
    <div>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div className="page-title">Xem tài liệu</div>
            <button className="back-btn back-btn-right" type="button" onClick={() => navigate(-1)}>
              ← Quay lại
            </button>
          </div>
          <div className="breadcrumb">
            <div className="crumb">User</div>
            <div className="crumb">Tài liệu</div>
            <div className="crumb">Xem</div>
          </div>
        </div>

        <div className="page-header-bottom">
          <div style={{ fontWeight: 800, color: "#0f172a" }}>{title}</div>
          <div className="header-actions">
            <Link className="btn" to={`/user/docs/${encodeURIComponent(chunkID)}`}>
              Chi tiết
            </Link>
            <button className="btn" type="button" onClick={onToggleSave} disabled={!doc}>
              {doc?.isSaved ? "★ Đã lưu" : "☆ Lưu"}
            </button>
            {viewUrl ? (
              <a className="btn" href={viewUrl} target="_blank" rel="noreferrer">
                Mở link
              </a>
            ) : null}
            {viewUrl && originalUrl && viewUrl !== originalUrl ? (
              <a className="btn" href={originalUrl} target="_blank" rel="noreferrer" title="File gốc">
                File gốc
              </a>
            ) : null}
          </div>
        </div>
      </div>

      {err ? <div style={{ marginBottom: 12, color: "#b91c1c", fontWeight: 700 }}>Lỗi: {err}</div> : null}
      {loading ? <div style={{ color: "#475569" }}>Đang tải…</div> : null}

      {!loading && doc && !viewUrl ? (
        <div className="empty-state">
          <div className="empty-state-icon">⚠️</div>
          <div>Không có URL để mở tài liệu.</div>
        </div>
      ) : null}

      {!loading && viewUrl ? (
        <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 12, overflow: "hidden" }}>
          {canIframe ? (
            <iframe title={title} src={viewUrl} style={{ width: "100%", height: "78vh", border: 0 }} allow="fullscreen" />
          ) : canImage ? (
            <div style={{ padding: 12, textAlign: "center" }}>
              <img src={viewUrl} alt={title} style={{ maxWidth: "100%", maxHeight: "78vh" }} />
            </div>
          ) : canVideo ? (
            <video controls style={{ width: "100%", maxHeight: "78vh" }} src={viewUrl} />
          ) : (
            <div style={{ padding: 16 }}>
              <div style={{ fontWeight: 800, color: "#0f172a", marginBottom: 6 }}>
                Không hỗ trợ xem trực tiếp định dạng “.{ext || "?"}”
              </div>
              <div style={{ color: "#475569", lineHeight: 1.6 }}>
                Bạn có thể bấm <strong>Mở link</strong> để tải xuống hoặc mở bằng ứng dụng phù hợp.
              </div>
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}
