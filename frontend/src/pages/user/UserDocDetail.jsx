import { useEffect, useMemo, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { getDocDetail, toggleSave } from "../../services/userDocsApi";
import DocumentCard from "../../components/DocumentCard";

function getTypeLabel(kind) {
  return (
    {
      document: "Chunk",
      chunk: "Chunk",
      class: "Lớp",
      subject: "Môn học",
      topic: "Chủ đề",
      lesson: "Bài học",
      image: "Hình ảnh",
      video: "Video",
    }[kind] || "Tài liệu"
  );
}

function detailHref(id, type) {
  return `/user/docs/${encodeURIComponent(id)}?type=${encodeURIComponent(type || "document")}`;
}

function viewHref(id, type) {
  return `/user/view/${encodeURIComponent(id)}?type=${encodeURIComponent(type || "document")}`;
}

function canOpenFile(doc, currentType) {
  const type = String(doc?.itemType || currentType || "document").toLowerCase();
  if (type === "class") return false;
  if (doc?.chunkUrl) return true;
  return Array.isArray(doc?.mappedDocuments) && doc.mappedDocuments.some((item) => item?.chunkUrl);
}

export default function UserDocDetail() {
  const { chunkID } = useParams();
  const [searchParams] = useSearchParams();
  const currentType = (searchParams.get("type") || "document").trim() || "document";
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [doc, setDoc] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError("");
        const detail = await getDocDetail(chunkID, { category: currentType });
        setDoc(detail);
      } catch (e) {
        setError(String(e?.message || e));
      } finally {
        setLoading(false);
      }
    })();
  }, [chunkID, currentType]);

  async function onToggleSave(currentDoc) {
    try {
      const category = currentDoc?.category || currentType || "document";
      const res = await toggleSave(currentDoc.chunkID, category);
      setDoc((prev) => (prev ? { ...prev, isSaved: !!res.saved } : prev));
    } catch (e) {
      setError(String(e?.message || e));
    }
  }

  const kindLabel = useMemo(() => getTypeLabel(doc?.itemType || currentType), [doc, currentType]);
  const hierarchy = [
    doc?.class?.classID
      ? { type: "class", id: doc.class.classID, label: doc.class.className || doc.class.classID }
      : null,
    doc?.subject?.subjectID
      ? {
          type: "subject",
          id: doc.subject.subjectID,
          label: doc.subject.subjectName || doc.subject.subjectID,
        }
      : null,
    doc?.topic?.topicID
      ? { type: "topic", id: doc.topic.topicID, label: doc.topic.topicName || doc.topic.topicID }
      : null,
    doc?.lesson?.lessonID
      ? { type: "lesson", id: doc.lesson.lessonID, label: doc.lesson.lessonName || doc.lesson.lessonID }
      : null,
  ].filter(Boolean);

  const openable = canOpenFile(doc, currentType);

  if (loading) return <div className="user-doc-empty">Đang tải chi tiết tài liệu...</div>;
  if (error) return <div className="user-doc-empty">{error}</div>;
  if (!doc) return <div className="user-doc-empty">Không có dữ liệu tài liệu.</div>;

  return (
    <div className="user-doc-view-shell">
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div className="page-title">Chi tiết {kindLabel.toLowerCase()}</div>
          </div>
          <div className="breadcrumb">
            <div className="crumb">User</div>
            <div className="crumb">Chi tiết</div>
            <div className="crumb active">{kindLabel}</div>
          </div>
        </div>
      </div>

      <div className="user-doc-view-toolbar">
        <div className="user-doc-view-toolbar-left">
          <Link className="btn" to="/user/search">
            Quay lại tìm kiếm
          </Link>
          <Link className="btn btn-primary" to={detailHref(doc.chunkID, doc.category || currentType)}>
            Đang xem chi tiết
          </Link>
          {openable ? (
            <Link className="btn" to={viewHref(doc.chunkID, doc.category || currentType)}>
              Mở file
            </Link>
          ) : null}
          {doc?.chunkUrl && String(doc?.itemType || currentType).toLowerCase() !== "class" ? (
            <a className="btn" href={doc.chunkUrl} target="_blank" rel="noreferrer">
              Tải tài liệu
            </a>
          ) : null}
        </div>
      </div>

      <DocumentCard doc={doc} onToggleSave={onToggleSave} />

      <div className="user-doc-detail-grid">
        <section className="user-doc-detail-panel">
          <h3>Cấu trúc được map</h3>
          {hierarchy.length ? (
            <div className="user-doc-related-list">
              {hierarchy.map((item) => (
                <Link
                  key={`${item.type}-${item.id}`}
                  to={detailHref(item.id, item.type)}
                  className="user-doc-related-link"
                >
                  <strong>{item.label}</strong>
                  <span>{getTypeLabel(item.type)}</span>
                </Link>
              ))}
            </div>
          ) : (
            <div className="user-doc-empty">Không có thông tin cấu trúc.</div>
          )}
        </section>

        <aside className="user-doc-detail-panel">
          <h3>Loại kết quả</h3>
          <div className="user-doc-keyword-list">
            <div className="user-doc-keyword-item">
              <strong>Kiểu:</strong> {kindLabel}
            </div>
            <div className="user-doc-keyword-item">
              <strong>ID:</strong> {doc.chunkID}
            </div>
            {doc?.chunkType ? (
              <div className="user-doc-keyword-item">
                <strong>Loại con:</strong> {doc.chunkType}
              </div>
            ) : null}
          </div>
        </aside>
      </div>

      <div className="user-doc-detail-grid">
        <section className="user-doc-detail-panel">
          <h3>Danh sách chunk được map</h3>
          {doc?.mappedDocuments?.length ? (
            <div className="user-doc-related-list">
              {doc.mappedDocuments.map((item) => (
                <div key={item.chunkID} className="user-doc-related-link">
                  <Link to={detailHref(item.chunkID, "document")}>
                    <strong>{item.chunkName || item.chunkID}</strong>
                  </Link>
                  <span>{item.lesson?.lessonName || item.lesson?.lessonID || ""}</span>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 8 }}>
                    <Link className="btn" to={detailHref(item.chunkID, "document")}>
                      Chi tiết
                    </Link>
                    {item?.chunkUrl ? (
                      <>
                        <Link className="btn btn-primary" to={viewHref(item.chunkID, "document")}>
                          Mở file
                        </Link>
                        <a className="btn" href={item.chunkUrl} target="_blank" rel="noreferrer">
                          Tải tài liệu
                        </a>
                      </>
                    ) : null}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="user-doc-empty">Không có chunk nào được map.</div>
          )}
        </section>

        <aside className="user-doc-detail-panel">
          <h3>Từ khóa</h3>
          {doc?.keywordItems?.length ? (
            <div className="user-doc-keyword-list">
              {doc.keywordItems.map((item, index) => (
                <div
                  className="user-doc-keyword-item"
                  key={`${item.keyword || item.keywordName || index}-${index}`}
                >
                  <strong>{item.keywordName || item.keyword || "Từ khóa"}</strong>
                </div>
              ))}
            </div>
          ) : (
            <div className="user-doc-empty">Mục này chưa có danh sách từ khóa riêng.</div>
          )}
        </aside>
      </div>
    </div>
  );
}
