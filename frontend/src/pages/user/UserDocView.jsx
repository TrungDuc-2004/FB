import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { getDocDetail, getDocView, toggleSave } from "../../services/userDocsApi";

export default function DocumentView() {
  const { chunkID } = useParams();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [doc, setDoc] = useState(null);
  const [viewUrl, setViewUrl] = useState("");

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError("");
        const [detail, view] = await Promise.all([getDocDetail(chunkID), getDocView(chunkID)]);
        setDoc(detail);
        setViewUrl(view.viewUrl || view.rawUrl || "");
      } catch (e) {
        setError(String(e?.message || e));
      } finally {
        setLoading(false);
      }
    })();
  }, [chunkID]);

  async function onToggleSave() {
    if (!doc?.chunkID) return;

    try {
      const res = await toggleSave(doc.chunkID, doc.chunkType || "document");
      setDoc((prev) => (prev ? { ...prev, isSaved: !!res.saved } : prev));
    } catch (e) {
      setError(String(e?.message || e));
    }
  }

  if (loading) return <div className="user-doc-empty">Đang tải tài liệu...</div>;
  if (error) return <div className="user-doc-empty">{error}</div>;
  if (!doc) return <div className="user-doc-empty">Không có dữ liệu tài liệu.</div>;

  return (
    <div className="user-doc-view-shell">
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div className="page-title">Xem tài liệu</div>
          </div>
          <div className="breadcrumb">
            <div className="crumb">User</div>
            <div className="crumb active">Xem tài liệu</div>
          </div>
        </div>
      </div>

      <div className="user-doc-view-toolbar">
        <div className="user-doc-view-toolbar-left">
          <Link className="btn" to="/user/library">
            Quay lại thư viện
          </Link>
          <Link className="btn" to={`/user/docs/${encodeURIComponent(doc.chunkID)}`}>
            Xem thông tin
          </Link>
        </div>

        <div className="user-doc-view-toolbar-right">
          {doc?.chunkUrl ? (
            <a className="btn" href={doc.chunkUrl} target="_blank" rel="noreferrer">
              Tải tài liệu
            </a>
          ) : null}
          <button className="btn btn-primary" type="button" onClick={onToggleSave}>
            {doc?.isSaved ? "Bỏ lưu" : "Lưu tài liệu"}
          </button>
        </div>
      </div>

      <div className="user-doc-detail-panel">
        <div className="user-doc-card-structure">
          <div className="user-doc-inline-meta">
            <span className="user-doc-inline-meta-label">Tên file</span>
            <span className="user-doc-inline-meta-value">{doc?.chunkName || doc?.chunkID}</span>
          </div>
          <div className="user-doc-inline-meta">
            <span className="user-doc-inline-meta-label">Môn</span>
            <span className="user-doc-inline-meta-value">
              {doc?.subject?.subjectName || doc?.subject?.subjectID || "—"}
            </span>
          </div>
          <div className="user-doc-inline-meta">
            <span className="user-doc-inline-meta-label">Chủ đề</span>
            <span className="user-doc-inline-meta-value">
              {doc?.topic?.topicName || doc?.topic?.topicID || "—"}
            </span>
          </div>
          <div className="user-doc-inline-meta">
            <span className="user-doc-inline-meta-label">Bài</span>
            <span className="user-doc-inline-meta-value">
              {doc?.lesson?.lessonName || doc?.lesson?.lessonID || "—"}
            </span>
          </div>
        </div>
      </div>

      <div className="user-doc-view-frame">
        {viewUrl ? (
          <iframe
            src={viewUrl}
            title={doc?.chunkName || doc?.chunkID || "Xem tài liệu"}
            allow="autoplay"
          />
        ) : (
          <div className="user-doc-empty">Không tìm thấy đường dẫn xem trực tiếp cho tài liệu này.</div>
        )}
      </div>
    </div>
  );
}