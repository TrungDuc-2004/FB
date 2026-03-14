import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { getDocDetail, getDocViewUrl, toggleSave } from "../../services/userDocsApi";

function getExt(url) {
  const u = (url || "").split("?")[0];
  const parts = u.split(".");
  return (parts[parts.length - 1] || "").toLowerCase();
}

function safeText(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) return text;
  }
  return "";
}

function getDirectUrl(doc, currentType) {
  const itemType = String(doc?.itemType || currentType || "document").toLowerCase();
  if (itemType === "class") return "";
  if (safeText(doc?.chunkUrl)) return safeText(doc.chunkUrl);
  if (["chunk", "document", "image", "video"].includes(itemType)) {
    const mapped = Array.isArray(doc?.mappedDocuments) ? doc.mappedDocuments : [];
    const firstWithUrl = mapped.find((item) => safeText(item?.chunkUrl));
    return safeText(firstWithUrl?.chunkUrl);
  }
  return "";
}

export default function DocumentView() {
  const { chunkID } = useParams();
  const [searchParams] = useSearchParams();
  const currentType = (searchParams.get("type") || "document").trim() || "document";
  const navigate = useNavigate();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [doc, setDoc] = useState(null);
  const [view, setView] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError("");
        const detail = await getDocDetail(chunkID, { category: currentType });
        setDoc(detail);

        try {
          const viewRes = await getDocViewUrl(chunkID, { category: currentType });
          setView(viewRes);
        } catch {
          setView(null);
        }
      } catch (e) {
        setError(String(e?.message || e));
      } finally {
        setLoading(false);
      }
    })();
  }, [chunkID, currentType]);

  async function onToggleSave() {
    try {
      if (!doc?.chunkID) return;
      const category = doc?.category || currentType || "document";
      const res = await toggleSave(doc.chunkID, category);
      setDoc((prev) => (prev ? { ...prev, isSaved: !!res.saved } : prev));
    } catch (e) {
      setError(String(e?.message || e));
    }
  }

  const originalUrl = useMemo(() => getDirectUrl(doc, currentType), [doc, currentType]);
  const viewUrl = view?.viewUrl || originalUrl;
  const ext = useMemo(() => getExt(viewUrl || originalUrl), [viewUrl, originalUrl]);
  const title = safeText(doc?.chunkName, doc?.chunkID, chunkID, "Xem tài liệu");
  const typeLabel = safeText(doc?.chunkType, doc?.itemType, currentType, "tài liệu");

  const canIframe = ["pdf"].includes(ext);
  const canImage = ["png", "jpg", "jpeg", "webp", "gif"].includes(ext);
  const canVideo = ["mp4", "webm", "ogg"].includes(ext);

  if (loading) return <div className="user-doc-empty">Đang tải tài liệu...</div>;
  if (error) return <div className="user-doc-empty">{error}</div>;
  if (!doc) return <div className="user-doc-empty">Không có dữ liệu tài liệu.</div>;

  return (
    <div className="doc-detail-page">
      <div className="doc-viewer-shell single-view">
        <div className="doc-viewer-toolbar">
          <div className="doc-viewer-toolbar-left">
            <button className="btn btn-ghost" type="button" onClick={() => navigate(-1)}>
              Quay lại
            </button>
            <Link className="btn btn-ghost" to={`/user/docs/${encodeURIComponent(chunkID)}?type=${encodeURIComponent(currentType)}`}>
              Xem chi tiết
            </Link>
          </div>

          <div className="doc-viewer-toolbar-center">
            <strong>{title}</strong>
            <span>{typeLabel}</span>
          </div>

          <div className="doc-viewer-toolbar-right">
            {originalUrl ? (
              <a className="btn btn-ghost" href={originalUrl} target="_blank" rel="noreferrer">
                Tải về
              </a>
            ) : null}
            <button className="btn btn-primary" type="button" onClick={onToggleSave}>
              {doc?.isSaved ? "Bỏ lưu" : "Lưu"}
            </button>
          </div>
        </div>

        <div className="doc-viewer-stage standalone">
          {!viewUrl ? (
            <div className="doc-preview-empty">Không có URL để xem trực tiếp tài liệu này.</div>
          ) : canIframe ? (
            <iframe title={title} src={viewUrl} allow="fullscreen" />
          ) : canImage ? (
            <div className="doc-preview-image-wrap">
              <img src={viewUrl} alt={title} />
            </div>
          ) : canVideo ? (
            <video controls src={viewUrl} className="doc-preview-video" />
          ) : (
            <div className="doc-preview-empty">
              Trình duyệt không hỗ trợ xem trực tiếp định dạng .{ext || "?"}. Hãy dùng nút <strong>Tải về</strong>.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
