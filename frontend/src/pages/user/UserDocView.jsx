import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";

import "../../styles/admin/page.css";
import * as api from "../../api/userDocsApi";

function extFromUrl(u = "") {
  const s = String(u || "");
  const noQ = s.split("?")[0];
  const i = noQ.lastIndexOf(".");
  return i >= 0 ? noQ.slice(i + 1).toLowerCase() : "";
}

export default function UserDocView() {
  const navigate = useNavigate();
  const { chunkId } = useParams();

  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [view, setView] = useState(null);

  useEffect(() => {
    let alive = true;
    async function load() {
      if (!chunkId) return;
      setLoading(true);
      setErr("");
      try {
        const d = await api.getChunkViewUrl(chunkId);
        if (!alive) return;
        setView(d);
      } catch (e) {
        if (!alive) return;
        setErr(String(e?.message || e));
      } finally {
        alive && setLoading(false);
      }
    }
    load();
    return () => {
      alive = false;
    };
  }, [chunkId]);

  const viewUrl = view?.view_url || view?.viewUrl || "";
  const originalUrl = view?.original_url || view?.originalUrl || "";
  const mode = view?.mode || "";
  const ext = (view?.ext || extFromUrl(viewUrl) || extFromUrl(originalUrl)).toLowerCase();

  const isPdf = extFromUrl(viewUrl) === "pdf" || ext === "pdf" || mode === "pdf_preview";
  const isImage = ["png", "jpg", "jpeg", "gif", "webp"].includes(extFromUrl(viewUrl) || ext);
  const isVideo = ["mp4", "webm", "mkv", "avi", "mov"].includes(extFromUrl(viewUrl) || ext);

  return (
    <div>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div>
              <div className="page-title">Xem tài liệu</div>
              <div className="breadcrumb">
                <span className="crumb">User</span>
                <span className="crumb">Tài liệu</span>
                <span className="crumb">Xem</span>
              </div>
            </div>
            <button className="back-btn back-btn-right" onClick={() => navigate(-1)} type="button">
              ← Quay lại
            </button>
          </div>
        </div>

        <div className="page-header-bottom">
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", width: "100%" }}>
            <div style={{ fontWeight: 800, color: "#0f172a" }}>{chunkId}</div>
            <button className="btn" type="button" onClick={() => navigate(`/user/docs/${encodeURIComponent(chunkId)}`)}>
              Chi tiết
            </button>
            {originalUrl ? (
              <a className="btn" href={originalUrl} target="_blank" rel="noreferrer">
                Tải file gốc
              </a>
            ) : null}
            {viewUrl && viewUrl !== originalUrl ? (
              <a className="btn btn-primary" href={viewUrl} target="_blank" rel="noreferrer">
                Mở PDF preview
              </a>
            ) : null}
          </div>
        </div>
      </div>

      <div className="table-wrapper" style={{ height: "74vh" }}>
        {err ? <div style={{ padding: 14, color: "#b91c1c" }}>Lỗi: {err}</div> : null}
        {loading && !view ? <div className="empty-state">Đang tải...</div> : null}

        {!loading && view && !viewUrl ? (
          <div className="empty-state">Không có URL để xem.</div>
        ) : null}

        {!loading && viewUrl ? (
          <div style={{ width: "100%", height: "100%" }}>
            {isPdf ? (
              <iframe
                title="pdf"
                src={viewUrl}
                style={{ width: "100%", height: "100%", border: 0 }}
              />
            ) : isImage ? (
              <div style={{ width: "100%", height: "100%", overflow: "auto", padding: 12 }}>
                <img src={viewUrl} alt="preview" style={{ maxWidth: "100%" }} />
              </div>
            ) : isVideo ? (
              <video src={viewUrl} controls style={{ width: "100%", height: "100%" }} />
            ) : (
              <div className="empty-state">
                Không preview được định dạng này. Hãy bấm "Tải file gốc".
              </div>
            )}
          </div>
        ) : null}
      </div>
    </div>
  );
}
