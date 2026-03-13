import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { getDocDetail, getDocViewUrl, toggleSave } from "../../services/userDocsApi";

function getExt(url) {
  const u = (url || "").split("?")[0];
  const parts = u.split(".");
  return (parts[parts.length - 1] || "").toLowerCase();
}

function detailHref(id, type) {
  return `/user/docs/${encodeURIComponent(id)}?type=${encodeURIComponent(type || "document")}`;
}

function getTypeLabel(kind) {
  return (
    {
      document: "Tài liệu",
      chunk: "Tài liệu",
      class: "Lớp",
      subject: "Môn học",
      topic: "Chủ đề",
      lesson: "Bài học",
      image: "Hình ảnh",
      video: "Video",
    }[kind] || "Tài liệu"
  );
}

function safeText(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) return text;
  }
  return "";
}

function buildHierarchy(doc) {
  return [
    doc?.lesson?.lessonID
      ? {
          kind: "lesson",
          id: doc.lesson.lessonID,
          title: safeText(doc?.lesson?.lessonName, doc?.lesson?.lessonID),
          subtitle: "Bài học",
        }
      : null,
    doc?.topic?.topicID
      ? {
          kind: "topic",
          id: doc.topic.topicID,
          title: safeText(doc?.topic?.topicName, doc?.topic?.topicID),
          subtitle: "Chủ đề",
        }
      : null,
    doc?.subject?.subjectID
      ? {
          kind: "subject",
          id: doc.subject.subjectID,
          title: safeText(doc?.subject?.subjectName, doc?.subject?.subjectID),
          subtitle: "Môn học",
        }
      : null,
  ].filter(Boolean);
}

function buildMedia(doc) {
  const images = Array.isArray(doc?.images)
    ? doc.images
        .filter((item) => safeText(item?.id, item?.url, item?.name))
        .map((item) => ({
          kind: "image",
          id: safeText(item?.id, item?.url, item?.name),
          title: safeText(item?.name, item?.id),
          subtitle: "Hình ảnh",
        }))
    : [];

  const videos = Array.isArray(doc?.videos)
    ? doc.videos
        .filter((item) => safeText(item?.id, item?.url, item?.name))
        .map((item) => ({
          kind: "video",
          id: safeText(item?.id, item?.url, item?.name),
          title: safeText(item?.name, item?.id),
          subtitle: "Video",
        }))
    : [];

  return [...images, ...videos];
}

export default function UserDocDetail() {
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
        const [detail, viewRes] = await Promise.all([
          getDocDetail(chunkID, { category: currentType }),
          getDocViewUrl(chunkID, { category: currentType }).catch(() => null),
        ]);
        setDoc(detail);
        setView(viewRes);
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

  const mappedFallbackUrl = Array.isArray(doc?.mappedDocuments)
    ? doc.mappedDocuments.find((item) => item?.chunkUrl)?.chunkUrl || ""
    : "";

  const originalUrl = currentType === "class" ? "" : doc?.chunkUrl || mappedFallbackUrl || "";
  const viewUrl = view?.viewUrl || originalUrl;
  const ext = useMemo(() => getExt(viewUrl || originalUrl), [viewUrl, originalUrl]);
  const kindLabel = useMemo(() => getTypeLabel(doc?.itemType || currentType), [doc, currentType]);
  const title = safeText(doc?.chunkName, doc?.chunkID, chunkID, "Chi tiết tài liệu");
  const description = safeText(
    doc?.chunkDescription,
    doc?.lesson?.lessonDescription,
    doc?.topic?.topicDescription,
    doc?.subject?.subjectDescription,
    "Nội dung chi tiết của tài liệu được hiển thị trong vùng xem ở giữa."
  );

  const canIframe = ["pdf"].includes(ext);
  const canImage = ["png", "jpg", "jpeg", "webp", "gif"].includes(ext);
  const canVideo = ["mp4", "webm", "ogg"].includes(ext);
  const isClass = String(doc?.itemType || currentType).toLowerCase() === "class";

  const hierarchyItems = buildHierarchy(doc || {});
  const mediaItems = buildMedia(doc || {});

  if (loading) return <div className="user-doc-empty">Đang tải chi tiết tài liệu...</div>;
  if (error) return <div className="user-doc-empty">{error}</div>;
  if (!doc) return <div className="user-doc-empty">Không có dữ liệu tài liệu.</div>;

  return (
    <div className="doc-detail-page">
      <div className="doc-detail-layout">
        <aside className="doc-side-card doc-side-left">
          <div className="doc-side-topline">{kindLabel}</div>
          <h1>{title}</h1>
          <p>{description}</p>

          <div className="doc-side-actions">
            <button className="btn btn-ghost" type="button" onClick={() => navigate(-1)}>
              Quay lại
            </button>
            <button className="btn btn-ghost" type="button" onClick={onToggleSave}>
              {doc?.isSaved ? "Bỏ lưu" : "Lưu"}
            </button>
            {viewUrl && !isClass ? (
              <a className="btn btn-primary" href={viewUrl} target="_blank" rel="noreferrer">
                Mở file
              </a>
            ) : null}
            {originalUrl && !isClass ? (
              <a className="btn btn-ghost" href={originalUrl} target="_blank" rel="noreferrer">
                Tải về
              </a>
            ) : null}
          </div>

          <div className="doc-side-section">
            <h3>Thông tin</h3>
            <div className="doc-side-meta-list">
              <div><span>ID</span><strong>{doc?.chunkID || "—"}</strong></div>
              <div><span>Loại</span><strong>{doc?.chunkType || kindLabel}</strong></div>
              <div><span>Lớp</span><strong>{doc?.class?.className || doc?.class?.classID || "—"}</strong></div>
              <div><span>Môn</span><strong>{doc?.subject?.subjectName || doc?.subject?.subjectID || "—"}</strong></div>
              <div><span>Chủ đề</span><strong>{doc?.topic?.topicName || doc?.topic?.topicID || "—"}</strong></div>
              <div><span>Bài</span><strong>{doc?.lesson?.lessonName || doc?.lesson?.lessonID || "—"}</strong></div>
            </div>
          </div>
        </aside>

        <main className="doc-viewer-panel">
          <div className="doc-viewer-toolbar simple">
            <div>
              <strong>{title}</strong>
              <span>Preview</span>
            </div>
          </div>

          <div className="doc-viewer-stage">
            {isClass ? (
              <div className="doc-preview-empty">Lớp không có file trực tiếp. Hãy xem bài học, chủ đề và môn học ở cột bên phải.</div>
            ) : !viewUrl ? (
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
                Trình duyệt không hỗ trợ xem trực tiếp định dạng .{ext || "?"}. Hãy dùng nút <strong>Mở file</strong> hoặc <strong>Tải về</strong>.
              </div>
            )}
          </div>
        </main>

        <aside className="doc-side-card doc-side-right">
          <div className="doc-side-section">
            <h3>Lesson · Topic · Subject</h3>
            <div className="doc-related-list hierarchy-only">
              {hierarchyItems.length ? (
                hierarchyItems.map((item) => (
                  <Link key={`${item.kind}-${item.id}`} className="doc-related-item" to={detailHref(item.id, item.kind)}>
                    <strong>{item.title}</strong>
                    <span>{item.subtitle}</span>
                  </Link>
                ))
              ) : (
                <div className="doc-related-empty">Chưa có lesson, topic hoặc subject liên quan.</div>
              )}
            </div>
          </div>

          {mediaItems.length ? (
            <div className="doc-side-section">
              <h3>Ảnh · Video liên quan</h3>
              <div className="doc-related-list">
                {mediaItems.map((item) => (
                  <Link key={`${item.kind}-${item.id}`} className="doc-related-item" to={detailHref(item.id, item.kind)}>
                    <strong>{item.title}</strong>
                    <span>{item.subtitle}</span>
                  </Link>
                ))}
              </div>
            </div>
          ) : null}
        </aside>
      </div>
    </div>
  );
}
