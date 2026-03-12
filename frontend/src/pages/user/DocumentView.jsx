import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import "../../styles/admin/page.css";
import { getDocDetail, getDocViewUrl, toggleSave } from "../../services/userDocsApi";

function getExt(url) {
  const u = (url || "").split("?")[0];
  const parts = u.split(".");
  return (parts[parts.length - 1] || "").toLowerCase();
}

function detailHref(id, type) {
  return `/user/docs/${encodeURIComponent(id)}?type=${encodeURIComponent(type || "document")}`;
}

export default function DocumentView() {
  const { chunkID } = useParams();
  const [searchParams] = useSearchParams();
  const currentType = (searchParams.get("type") || "document").trim() || "document";
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
        getDocDetail(chunkID, { category: currentType }),
        getDocViewUrl(chunkID, { category: currentType }).catch(() => null),
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
  }, [chunkID, currentType]);

  async function onToggleSave() {
    try {
      const r = await toggleSave(chunkID, currentType);
      setDoc((prev) => (prev ? { ...prev, isSaved: r.saved } : prev));
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  const mappedFallbackUrl = Array.isArray(doc?.mappedDocuments)
    ? doc.mappedDocuments.find((item) => item?.chunkUrl)?.chunkUrl || ""
    : "";

  const originalUrl =
    String(doc?.itemType || currentType).toLowerCase() === "class"
      ? ""
      : doc?.chunkUrl || mappedFallbackUrl || "";

  const viewUrl = view?.viewUrl || originalUrl;
  const ext = useMemo(() => getExt(viewUrl || originalUrl), [viewUrl, originalUrl]);
  const title = doc?.chunkName || chunkID;

  const canIframe = ["pdf"].includes(ext);
  const canImage = ["png", "jpg", "jpeg", "webp", "gif"].includes(ext);
  const canVideo = ["mp4", "webm", "ogg"].includes(ext);
  const isOfficeLike = ["doc", "docx", "ppt", "pptx", "xls", "xlsx"].includes(ext);
  const isClass = String(doc?.itemType || currentType).toLowerCase() === "class";

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div>
              <div className="page-title">Xem tài liệu</div>
              <div className="breadcrumb">
                <div className="crumb">User</div>
                <div className="crumb">Tài liệu</div>
                <div className="crumb">Xem</div>
              </div>
            </div>
            <button className="back-btn back-btn-right" type="button" onClick={() => navigate(-1)}>
              ← Quay lại
            </button>
          </div>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "minmax(0, 1.4fr) minmax(280px, 1fr)",
            gap: 16,
          }}
        >
          <div
            style={{
              border: "1px solid #dbeafe",
              borderRadius: 18,
              padding: 18,
              background: "linear-gradient(180deg, #f8fbff 0%, #ffffff 100%)",
            }}
          >
            <div style={{ fontSize: 12.5, color: "#64748b", marginBottom: 8 }}>Đang xem</div>
            <div style={{ fontSize: 24, fontWeight: 800, color: "#0f172a", marginBottom: 8 }}>{title}</div>
            <div style={{ color: "#475569", lineHeight: 1.7 }}>
              {doc?.chunkDescription ||
                "Nếu trình xem trực tiếp không hỗ trợ định dạng này, bạn vẫn có thể mở link gốc hoặc tải file về."}
            </div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 14 }}>
              {doc?.class?.classID ? (
                <Link className="crumb" to={detailHref(doc.class.classID, "class")}>
                  {doc?.class?.className || doc?.class?.classID}
                </Link>
              ) : null}
              {doc?.subject?.subjectID ? (
                <Link className="crumb" to={detailHref(doc.subject.subjectID, "subject")}>
                  {doc?.subject?.subjectName || doc?.subject?.subjectID}
                </Link>
              ) : null}
              {doc?.topic?.topicID ? (
                <Link className="crumb" to={detailHref(doc.topic.topicID, "topic")}>
                  {doc?.topic?.topicName || doc?.topic?.topicID}
                </Link>
              ) : null}
              {doc?.lesson?.lessonID ? (
                <Link className="crumb" to={detailHref(doc.lesson.lessonID, "lesson")}>
                  {doc?.lesson?.lessonName || doc?.lesson?.lessonID}
                </Link>
              ) : null}
              <span className="crumb">.{ext || "?"}</span>
            </div>
          </div>

          <div
            style={{
              border: "1px solid #e2e8f0",
              borderRadius: 18,
              padding: 18,
              background: "#fff",
              display: "grid",
              gap: 12,
            }}
          >
            <div style={{ color: "#334155", lineHeight: 1.7 }}>
              <div>
                <b>Bài học:</b> {doc?.lesson?.lessonName || doc?.lesson?.lessonID || "-"}
              </div>
              <div>
                <b>Loại:</b> {doc?.chunkType || doc?.lesson?.lessonType || doc?.itemType || "-"}
              </div>
              <div>
                <b>Trạng thái lưu:</b> {doc?.isSaved ? "Đã lưu" : "Chưa lưu"}
              </div>
            </div>
            <div className="header-actions">
              <Link className="btn" to={detailHref(chunkID, currentType)}>
                Chi tiết
              </Link>
              <button className="btn" type="button" onClick={onToggleSave} disabled={!doc}>
                {doc?.isSaved ? "★ Đã lưu" : "☆ Lưu"}
              </button>
              {viewUrl && !isClass ? (
                <a className="btn btn-primary" href={viewUrl} target="_blank" rel="noreferrer">
                  Mở link
                </a>
              ) : null}
              {originalUrl && !isClass ? (
                <a className="btn" href={originalUrl} target="_blank" rel="noreferrer">
                  Tải tài liệu
                </a>
              ) : null}
              {viewUrl && originalUrl && viewUrl !== originalUrl && !isClass ? (
                <a className="btn" href={originalUrl} target="_blank" rel="noreferrer" title="File gốc">
                  File gốc
                </a>
              ) : null}
            </div>
          </div>
        </div>
      </div>

      {err ? <div style={{ color: "#b91c1c", fontWeight: 700 }}>Lỗi: {err}</div> : null}
      {loading ? <div style={{ color: "#475569" }}>Đang tải…</div> : null}

      {!loading && doc && isClass ? (
        <div className="empty-state">
          <div className="empty-state-icon">ℹ️</div>
          <div>Lớp không có file trực tiếp. Hãy xem các môn, chủ đề, bài học hoặc chunk được map phía dưới.</div>
        </div>
      ) : null}

      {!loading && doc && !viewUrl && !isClass ? (
        <div className="empty-state">
          <div className="empty-state-icon">⚠️</div>
          <div>Không có URL để mở tài liệu.</div>
        </div>
      ) : null}

      {!loading && viewUrl && !isClass ? (
        <div
          style={{
            background: "#fff",
            border: "1px solid #e2e8f0",
            borderRadius: 16,
            overflow: "hidden",
          }}
        >
          {canIframe ? (
            <iframe title={title} src={viewUrl} style={{ width: "100%", height: "78vh", border: 0 }} allow="fullscreen" />
          ) : canImage ? (
            <div style={{ padding: 12, textAlign: "center" }}>
              <img src={viewUrl} alt={title} style={{ maxWidth: "100%", maxHeight: "78vh" }} />
            </div>
          ) : canVideo ? (
            <video controls style={{ width: "100%", maxHeight: "78vh" }} src={viewUrl} />
          ) : (
            <div style={{ padding: 18 }}>
              <div style={{ fontWeight: 800, color: "#0f172a", marginBottom: 8 }}>
                Không hỗ trợ xem trực tiếp định dạng “.{ext || "?"}”
              </div>
              <div style={{ color: "#475569", lineHeight: 1.7 }}>
                {isOfficeLike
                  ? "Định dạng Office thường không nhúng ổn định trong iframe của trình duyệt. Hãy dùng nút Mở link hoặc File gốc để mở bằng ứng dụng phù hợp."
                  : "Bạn có thể bấm Mở link để tải xuống hoặc mở bằng ứng dụng phù hợp."}
              </div>
            </div>
          )}
        </div>
      ) : null}

      {!loading && Array.isArray(doc?.mappedDocuments) && doc.mappedDocuments.length > 0 ? (
        <div className="user-doc-detail-panel">
          <h3>Chunk được map</h3>
          <div className="user-doc-related-list">
            {doc.mappedDocuments.map((item) => (
              <div key={item.chunkID} className="user-doc-related-link">
                <Link to={detailHref(item.chunkID, "document")}>
                  <strong>{item.chunkName || item.chunkID}</strong>
                </Link>
                <span>{item.lesson?.lessonName || item.lesson?.lessonID || ""}</span>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
