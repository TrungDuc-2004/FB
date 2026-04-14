/* eslint-disable react-refresh/only-export-components */
import { Link } from "react-router-dom";

function safeText(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) return text;
  }
  return "";
}

function normalizeDescriptionText(value = "") {
  if (value && typeof value === "object") {
    return normalizeDescriptionText(value.description || value.caption || value.text || "");
  }

  let text = String(value ?? "").trim();
  if (!text) return "";

  const match = text.match(/['"]description['"]\s*:\s*['"]([\s\S]*?)['"]\s*[,}]/i);
  if (match?.[1]) {
    text = match[1].trim();
  }

  text = text.replace(/^\s*['"]?description['"]?\s*:\s*/i, "");
  text = text.replace(/\n/g, " ").replace(/\r/g, " ");
  text = text.replace(/^\{+|\}+$/g, "").trim();
  text = text.replace(/\s+/g, " ").trim();
  text = text.replace(/^['"]+|['"]+$/g, "").trim();
  return text;
}

function shortText(value, maxLength = 220) {
  const text = normalizeDescriptionText(value);
  if (!text) return "";
  return text.length > maxLength ? `${text.slice(0, maxLength - 1)}…` : text;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function getItemType(doc) {
  const raw = doc?.itemType || doc?.type || doc?.category || "chunk";
  return raw === "document" ? "chunk" : String(raw).toLowerCase();
}

function getSaveCategory(doc) {
  const kind = getItemType(doc);
  return kind === "chunk" ? "document" : kind;
}

function getDocId(doc) {
  return safeText(doc?.chunkID, doc?.id);
}

function getDetailTo(doc) {
  const id = getDocId(doc);
  if (!id) return null;
  return `/user/docs/${encodeURIComponent(id)}?type=${encodeURIComponent(getSaveCategory(doc))}`;
}

function getViewTo(doc) {
  const id = getDocId(doc);
  if (!id) return null;
  return `/user/view/${encodeURIComponent(id)}?type=${encodeURIComponent(getSaveCategory(doc))}`;
}

function getResolvedFileUrl(doc) {
  if (getItemType(doc) === "class") return "";
  if (safeText(doc?.chunkUrl)) return safeText(doc.chunkUrl);

  const mapped = safeArray(doc?.mappedDocuments);
  const firstWithUrl = mapped.find((item) => safeText(item?.chunkUrl));
  return safeText(firstWithUrl?.chunkUrl);
}

function getTypeLabel(kind) {
  return (
    {
      chunk: "PDF",
      class: "CLASS",
      subject: "SUBJECT",
      topic: "TOPIC",
      lesson: "LESSON",
      image: "IMAGE",
      video: "VIDEO",
    }[kind] || "FILE"
  );
}

function getTypeTitle(kind) {
  return (
    {
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

function getCoverText(kind) {
  return (
    {
      chunk: "PDF",
      class: "LỚP",
      subject: "MH",
      topic: "CD",
      lesson: "BÀI",
      image: "IMG",
      video: "VID",
    }[kind] || "FILE"
  );
}

function getPreviewImage(doc) {
  const kind = getItemType(doc);
  if (kind === "image" && safeText(doc?.chunkUrl)) {
    return safeText(doc.chunkUrl);
  }
  return "";
}

function MetaGroup({ title, value, to }) {
  if (!value) return null;
  return (
    <div className="user-doc-meta-chip">
      <span>{title}</span>
      {to ? <Link to={to}>{value}</Link> : <strong>{value}</strong>}
    </div>
  );
}

function buildStructureItems(doc) {
  const items = [];

  const subjectId = safeText(doc?.subject?.subjectID);
  const subjectName = safeText(doc?.subject?.subjectName, subjectId);
  if (subjectName) items.push({ kind: "subject", id: subjectId || subjectName, title: subjectName });

  const topicId = safeText(doc?.topic?.topicID);
  const topicName = safeText(doc?.topic?.topicName, topicId);
  if (topicName) items.push({ kind: "topic", id: topicId || topicName, title: topicName });

  const lessonId = safeText(doc?.lesson?.lessonID);
  const lessonName = safeText(doc?.lesson?.lessonName, lessonId);
  if (lessonName) items.push({ kind: "lesson", id: lessonId || lessonName, title: lessonName });

  return items;
}

export function normalizeRelatedItems(doc) {
  const safeDoc = doc && typeof doc === "object" ? doc : {};
  const items = [];

  for (const entry of buildStructureItems(safeDoc)) {
    items.push({
      key: `${entry.kind}-${entry.id}`,
      id: entry.id,
      kind: entry.kind,
      title: entry.title,
      description: `${getTypeTitle(entry.kind)} liên quan tới nội dung hiện tại.`,
      href: "",
      chunkID: entry.id,
      itemType: entry.kind,
      category: entry.kind,
      class: safeDoc?.class || { classID: "", className: "" },
      subject: safeDoc?.subject || { subjectID: "", subjectName: "", subjectUrl: "" },
      topic: safeDoc?.topic || { topicID: "", topicName: "", topicUrl: "" },
      lesson: safeDoc?.lesson || { lessonID: "", lessonName: "", lessonType: "", lessonUrl: "" },
      mappedDocuments: safeArray(safeDoc?.mappedDocuments),
    });
  }

  for (const item of safeArray(safeDoc?.images).slice(0, 6)) {
    const imageId = safeText(item?.id, item?.url, item?.name);
    if (!imageId) continue;
    items.push({
      key: `image-${imageId}`,
      id: imageId,
      kind: "image",
      title: safeText(item?.name, imageId),
      description: shortText(item?.description || "Hình ảnh liên quan."),
      href: safeText(item?.url),
      chunkID: imageId,
      chunkName: safeText(item?.name, imageId),
      chunkUrl: safeText(item?.url),
      itemType: "image",
      category: "image",
      class: safeDoc?.class || { classID: "", className: "" },
      subject: safeDoc?.subject || { subjectID: "", subjectName: "", subjectUrl: "" },
      topic: safeDoc?.topic || { topicID: "", topicName: "", topicUrl: "" },
      lesson: safeDoc?.lesson || { lessonID: "", lessonName: "", lessonType: "", lessonUrl: "" },
      mappedDocuments: safeArray(safeDoc?.mappedDocuments),
    });
  }

  for (const item of safeArray(safeDoc?.videos).slice(0, 6)) {
    const videoId = safeText(item?.id, item?.url, item?.name);
    if (!videoId) continue;
    items.push({
      key: `video-${videoId}`,
      id: videoId,
      kind: "video",
      title: safeText(item?.name, videoId),
      description: shortText(item?.description || "Video liên quan."),
      href: safeText(item?.url),
      chunkID: videoId,
      chunkName: safeText(item?.name, videoId),
      chunkUrl: safeText(item?.url),
      itemType: "video",
      category: "video",
      class: safeDoc?.class || { classID: "", className: "" },
      subject: safeDoc?.subject || { subjectID: "", subjectName: "", subjectUrl: "" },
      topic: safeDoc?.topic || { topicID: "", topicName: "", topicUrl: "" },
      lesson: safeDoc?.lesson || { lessonID: "", lessonName: "", lessonType: "", lessonUrl: "" },
      mappedDocuments: safeArray(safeDoc?.mappedDocuments),
    });
  }

  return items;
}

export function RelatedEntityCard({ item, onToggleSave }) {
  const safeItem = item && typeof item === "object" ? item : {};
  const kind = getItemType(safeItem);
  const detailTo = getDetailTo(safeItem);
  const viewTo = getViewTo(safeItem);
  const resolvedFileUrl = getResolvedFileUrl(safeItem);

  return (
    <article className="related-entity-card">
      <div className="related-entity-badge">{getTypeTitle(kind)}</div>
      {detailTo ? (
        <Link className="related-entity-title" to={detailTo}>
          {safeText(safeItem?.title, safeItem?.chunkName, safeItem?.chunkID, "Chưa có tên")}
        </Link>
      ) : (
        <div className="related-entity-title">{safeText(safeItem?.title, safeItem?.chunkName, safeItem?.chunkID, "Chưa có tên")}</div>
      )}
      {safeItem?.description ? <p>{shortText(safeItem.description, 90)}</p> : null}
      <div className="related-entity-actions">
        {detailTo ? <Link to={detailTo}>Chi tiết</Link> : null}
        {viewTo && resolvedFileUrl ? <Link to={viewTo}>Mở</Link> : null}
        {!viewTo && safeItem?.href ? (
          <a href={safeItem.href} target="_blank" rel="noreferrer">Xem</a>
        ) : null}
        {typeof onToggleSave === "function" ? (
          <button type="button" onClick={() => onToggleSave(safeItem)}>
            {safeItem?.isSaved ? "Bỏ lưu" : "Lưu"}
          </button>
        ) : null}
      </div>
    </article>
  );
}

export function DocumentMappedItems({ doc, onToggleSave }) {
  const items = normalizeRelatedItems(doc);
  if (!items.length) return null;

  return (
    <div className="user-doc-related-grid">
      {items.map((item) => (
        <RelatedEntityCard key={item.key} item={item} onToggleSave={onToggleSave} />
      ))}
    </div>
  );
}

export default function DocumentCard({ doc, onToggleSave, variant = "default" }) {
  const safeDoc = doc && typeof doc === "object" ? doc : {};
  const kind = getItemType(safeDoc);
  const name = safeText(safeDoc?.chunkName, safeDoc?.name, safeDoc?.chunkID, "Tài liệu chưa đặt tên");
  const desc = shortText(
    safeDoc?.chunkDescription ||
      safeDoc?.description ||
      safeDoc?.topic?.topicDescription ||
      safeDoc?.subject?.subjectDescription ||
      safeDoc?.lesson?.lessonDescription,
    260
  );
  const detailTo = getDetailTo(safeDoc);
  const viewTo = getViewTo(safeDoc);
  const resolvedFileUrl = getResolvedFileUrl(safeDoc);
  const previewImage = getPreviewImage(safeDoc);
  const saveCategory = getSaveCategory(safeDoc);
  const showOpen = kind !== "class" && Boolean(viewTo && resolvedFileUrl);
  const showDownload = kind !== "class" && Boolean(resolvedFileUrl);

  const subjectId = safeText(safeDoc?.subject?.subjectID);
  const topicId = safeText(safeDoc?.topic?.topicID);
  const lessonId = safeText(safeDoc?.lesson?.lessonID);

  const subjectName = safeText(safeDoc?.subject?.subjectName, subjectId);
  const topicName = safeText(safeDoc?.topic?.topicName, topicId);
  const lessonName = safeText(safeDoc?.lesson?.lessonName, lessonId);

  const subjectTo = subjectId ? `/user/docs/${encodeURIComponent(subjectId)}?type=subject` : null;
  const topicTo = topicId ? `/user/docs/${encodeURIComponent(topicId)}?type=topic` : null;
  const lessonTo = lessonId ? `/user/docs/${encodeURIComponent(lessonId)}?type=lesson` : null;

  const relatedImageCount = safeArray(safeDoc?.images).length;
  const relatedVideoCount = safeArray(safeDoc?.videos).length;
  const mappedCount = safeArray(safeDoc?.mappedDocuments).length;

  return (
    <article className="user-doc-card user-doc-card-scribd">
      <div className="user-doc-thumb-col">
        {detailTo ? (
          <Link className="user-doc-preview-link" to={detailTo}>
            <div
              className={`user-doc-thumb user-doc-thumb-${kind}${previewImage ? " has-image" : ""}`}
              style={previewImage ? { backgroundImage: `url(${previewImage})` } : undefined}
            >
              <span className="user-doc-thumb-type">{getTypeLabel(kind)}</span>
              {!previewImage ? <span className="user-doc-thumb-text">{getCoverText(kind)}</span> : null}
            </div>
          </Link>
        ) : (
          <div
            className={`user-doc-thumb user-doc-thumb-${kind}${previewImage ? " has-image" : ""}`}
            style={previewImage ? { backgroundImage: `url(${previewImage})` } : undefined}
          >
            <span className="user-doc-thumb-type">{getTypeLabel(kind)}</span>
            {!previewImage ? <span className="user-doc-thumb-text">{getCoverText(kind)}</span> : null}
          </div>
        )}
      </div>

      <div className="user-doc-card-main">
        <div className="user-doc-card-head compact scribd-head">
          <div className="user-doc-card-title-wrap">
            <div className="user-doc-top-metrics">
              <span>{getTypeTitle(kind)}</span>
              {safeDoc?.score ? <span>{Math.round(Number(safeDoc.score) * 100)}%</span> : null}
              {safeDoc?.chunkType ? <span>{safeDoc.chunkType}</span> : null}
            </div>

            {detailTo ? (
              <Link className="user-doc-card-title link" to={detailTo}>
                {name}
              </Link>
            ) : (
              <div className="user-doc-card-title">{name}</div>
            )}

            {desc ? <p className="user-doc-card-desc">{desc}</p> : null}
          </div>

          <div className="user-doc-card-actions user-doc-card-actions-desktop compact scribd-actions">
            {showOpen ? <Link className="btn btn-ghost" to={viewTo}>Mở</Link> : null}
            {showDownload ? <a className="btn btn-ghost" href={resolvedFileUrl} target="_blank" rel="noreferrer">Tải</a> : null}
            {typeof onToggleSave === "function" ? (
              <button className="btn btn-ghost" type="button" onClick={() => onToggleSave({ ...safeDoc, category: saveCategory })}>
                {safeDoc?.isSaved ? "Bỏ lưu" : "Lưu"}
              </button>
            ) : null}
          </div>
        </div>

        {variant !== "search" ? (
          <div className="user-doc-card-structure user-doc-card-structure-compact user-doc-card-structure-list">
            <MetaGroup title="Môn" value={subjectName} to={subjectTo} />
            <MetaGroup title="Chủ đề" value={topicName} to={topicTo} />
            <MetaGroup title="Bài" value={lessonName} to={lessonTo} />
          </div>
        ) : null}

        <div className="user-doc-bottom-row">
          <div className="user-doc-bottom-tags">
            {relatedImageCount ? <span>{relatedImageCount} ảnh liên quan</span> : null}
            {relatedVideoCount ? <span>{relatedVideoCount} video liên quan</span> : null}
            {mappedCount && kind !== "chunk" ? <span>{mappedCount} tài liệu map</span> : null}
            {!relatedImageCount && !relatedVideoCount && mappedCount && kind === "chunk" ? <span>{mappedCount} nội dung liên kết</span> : null}
          </div>

          <div className="user-doc-card-actions user-doc-card-actions-mobile compact scribd-actions-mobile">
            {showOpen ? <Link className="btn btn-ghost" to={viewTo}>Mở</Link> : null}
            {showDownload ? <a className="btn btn-ghost" href={resolvedFileUrl} target="_blank" rel="noreferrer">Tải</a> : null}
            {typeof onToggleSave === "function" ? (
              <button className="btn btn-ghost" type="button" onClick={() => onToggleSave({ ...safeDoc, category: saveCategory })}>
                {safeDoc?.isSaved ? "Bỏ lưu" : "Lưu"}
              </button>
            ) : null}
          </div>
        </div>
      </div>
    </article>
  );
}
