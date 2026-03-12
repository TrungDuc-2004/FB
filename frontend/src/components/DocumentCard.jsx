import { Link } from "react-router-dom";

function safeText(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) return text;
  }
  return "";
}

function shortText(value, maxLength = 180) {
  const text = safeText(value);
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

function getTypeLabel(kind) {
  return (
    {
      chunk: "CHUNK",
      class: "CLASS",
      subject: "SUBJECT",
      topic: "TOPIC",
      lesson: "LESSON",
      image: "IMAGE",
      video: "VIDEO",
    }[kind] || "TÀI LIỆU"
  );
}

function getAvatarLabel(kind) {
  return (
    {
      chunk: "CK",
      class: "CLA",
      subject: "MH",
      topic: "CD",
      lesson: "B",
      image: "Ả",
      video: "VD",
    }[kind] || "TL"
  );
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

function toneByKind(kind) {
  switch (String(kind || "").toLowerCase()) {
    case "class":
      return "blue";
    case "subject":
      return "indigo";
    case "topic":
      return "violet";
    case "lesson":
      return "green";
    case "image":
      return "amber";
    case "video":
      return "orange";
    default:
      return "slate";
  }
}

function kindMeta(kind) {
  switch (String(kind || "").toLowerCase()) {
    case "class":
      return { label: "Lớp", avatar: "L" };
    case "subject":
      return { label: "Môn học", avatar: "MH" };
    case "topic":
      return { label: "Chủ đề", avatar: "CD" };
    case "lesson":
      return { label: "Bài học", avatar: "B" };
    case "image":
      return { label: "Hình ảnh", avatar: "Ả" };
    case "video":
      return { label: "Video", avatar: "VD" };
    default:
      return { label: "Tài liệu", avatar: "TL" };
  }
}

function MetaGroup({ title, value, to }) {
  if (!value) return null;

  return (
    <div className={`user-doc-inline-meta${to ? " is-link" : ""}`}>
      <span className="user-doc-inline-meta-label">{title}</span>
      {to ? (
        <Link className="user-doc-inline-meta-value user-doc-inline-meta-value-link" to={to}>
          {value}
        </Link>
      ) : (
        <span className="user-doc-inline-meta-value">{value}</span>
      )}
    </div>
  );
}

export function normalizeRelatedItems(doc) {
  const safeDoc = doc && typeof doc === "object" ? doc : {};
  const items = [];

  const classId = safeText(safeDoc?.class?.classID);
  const className = safeText(safeDoc?.class?.className, classId);
  if (className) {
    items.push({
      key: `class-${classId || className}`,
      id: classId || className,
      kind: "class",
      title: className,
      subtitle: classId && classId !== className ? classId : "",
      description: shortText(
        safeDoc?.class?.classDescription || "Thông tin lớp được gắn với tài liệu này.",
        90
      ),
      href: "",
    });
  }

  const subjectId = safeText(safeDoc?.subject?.subjectID);
  const subjectName = safeText(safeDoc?.subject?.subjectName, subjectId);
  if (subjectName) {
    items.push({
      key: `subject-${subjectId || subjectName}`,
      id: subjectId || subjectName,
      kind: "subject",
      title: subjectName,
      subtitle: subjectId && subjectId !== subjectName ? subjectId : "",
      description: shortText(
        safeDoc?.subject?.subjectDescription || "Môn học được map theo chunk này.",
        90
      ),
      href: safeText(safeDoc?.subject?.subjectUrl),
      mappedDocuments: safeArray(safeDoc?.mappedDocuments),
    });
  }

  const topicId = safeText(safeDoc?.topic?.topicID);
  const topicName = safeText(safeDoc?.topic?.topicName, topicId);
  if (topicName) {
    items.push({
      key: `topic-${topicId || topicName}`,
      id: topicId || topicName,
      kind: "topic",
      title: topicName,
      subtitle: topicId && topicId !== topicName ? topicId : "",
      description: shortText(
        safeDoc?.topic?.topicDescription || "Chủ đề được map theo chunk này.",
        90
      ),
      href: safeText(safeDoc?.topic?.topicUrl),
      mappedDocuments: safeArray(safeDoc?.mappedDocuments),
    });
  }

  const lessonId = safeText(safeDoc?.lesson?.lessonID);
  const lessonName = safeText(safeDoc?.lesson?.lessonName, lessonId);
  if (lessonName) {
    items.push({
      key: `lesson-${lessonId || lessonName}`,
      id: lessonId || lessonName,
      kind: "lesson",
      title: lessonName,
      subtitle: lessonId && lessonId !== lessonName ? lessonId : "",
      description: shortText(
        safeDoc?.lesson?.lessonDescription || "Bài học được map theo chunk này.",
        90
      ),
      href: safeText(safeDoc?.lesson?.lessonUrl),
      mappedDocuments: safeArray(safeDoc?.mappedDocuments),
    });
  }

  for (const item of safeArray(safeDoc?.images)) {
    const imageId = safeText(item?.id);
    const title = safeText(item?.name, imageId, item?.url);
    if (!title) continue;

    items.push({
      key: `image-${imageId || item?.url || title}`,
      id: imageId || title,
      kind: "image",
      title,
      subtitle: imageId && imageId !== title ? imageId : "",
      description: shortText(item?.description || "Hình ảnh liên quan tới chunk này.", 90),
      href: safeText(item?.url),
      mappedDocuments: safeArray(safeDoc?.mappedDocuments),
      chunkUrl: safeText(item?.url),
    });
  }

  for (const item of safeArray(safeDoc?.videos)) {
    const videoId = safeText(item?.id);
    const title = safeText(item?.name, videoId, item?.url);
    if (!title) continue;

    items.push({
      key: `video-${videoId || item?.url || title}`,
      id: videoId || title,
      kind: "video",
      title,
      subtitle: videoId && videoId !== title ? videoId : "",
      description: shortText(item?.description || "Video liên quan tới chunk này.", 90),
      href: safeText(item?.url),
      mappedDocuments: safeArray(safeDoc?.mappedDocuments),
      chunkUrl: safeText(item?.url),
    });
  }

  return items;
}

export function RelatedEntityCard({ item, onToggleSave }) {
  const safeItem = item && typeof item === "object" ? item : {};
  const kind = safeText(safeItem?.kind, "chunk").toLowerCase();
  const meta = kindMeta(kind);
  const detailTo = getDetailTo(safeItem);
  const viewTo = getViewTo(safeItem);
  const resolvedFileUrl = getResolvedFileUrl(safeItem);
  const isSaved = Boolean(safeItem?.isSaved);

  const showOpen = kind !== "class" && Boolean(viewTo && resolvedFileUrl);
  const showDownload = kind !== "class" && Boolean(resolvedFileUrl);

  return (
    <article className="user-doc-card user-doc-card-clean">
      {detailTo ? (
        <Link className="user-doc-preview-link" to={detailTo}>
          <div className={`user-doc-file-avatar user-doc-file-avatar-${kind}`}>{meta.avatar}</div>
        </Link>
      ) : (
        <div className="user-doc-preview-link">
          <div className={`user-doc-file-avatar user-doc-file-avatar-${kind}`}>{meta.avatar}</div>
        </div>
      )}

      <div className="user-doc-card-main">
        <div className="user-doc-card-head compact">
          <div className="user-doc-card-title-wrap">
            {detailTo ? (
              <Link className="user-doc-card-title link" to={detailTo}>
                {safeText(safeItem?.title, "Chưa có tên")}
              </Link>
            ) : (
              <div className="user-doc-card-title">{safeText(safeItem?.title, "Chưa có tên")}</div>
            )}

            <div className="user-doc-card-badges">
              <span className="user-doc-badge">{meta.label}</span>
              {safeItem?.subtitle ? (
                <span className="user-doc-badge user-doc-badge-subtle">{safeText(safeItem.subtitle)}</span>
              ) : null}
            </div>
          </div>

          <div className="user-doc-card-actions user-doc-card-actions-desktop compact">
            {detailTo ? (
              <Link className="btn btn-primary" to={detailTo}>
                Chi tiết
              </Link>
            ) : null}

            {showOpen ? (
              <Link className="btn" to={viewTo}>
                Mở file
              </Link>
            ) : null}

            {showDownload ? (
              <a className="btn" href={resolvedFileUrl} target="_blank" rel="noreferrer">
                Tải tài liệu
              </a>
            ) : null}

            {typeof onToggleSave === "function" ? (
              <button className="btn" type="button" onClick={() => onToggleSave(safeItem)}>
                {isSaved ? "Bỏ lưu" : "Lưu"}
              </button>
            ) : null}
          </div>
        </div>

        {safeItem?.description ? (
          <p className="user-doc-card-desc">{safeText(safeItem.description)}</p>
        ) : null}

        <div className="user-doc-card-actions user-doc-card-actions-mobile compact">
          {detailTo ? (
            <Link className="btn btn-primary" to={detailTo}>
              Chi tiết
            </Link>
          ) : null}

          {showOpen ? (
            <Link className="btn" to={viewTo}>
              Mở file
            </Link>
          ) : null}

          {showDownload ? (
            <a className="btn" href={resolvedFileUrl} target="_blank" rel="noreferrer">
              Tải tài liệu
            </a>
          ) : null}

          {typeof onToggleSave === "function" ? (
            <button className="btn" type="button" onClick={() => onToggleSave(safeItem)}>
              {isSaved ? "Bỏ lưu" : "Lưu"}
            </button>
          ) : null}
        </div>
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

export default function DocumentCard({ doc, onToggleSave }) {
  const safeDoc = doc && typeof doc === "object" ? doc : {};
  const kind = getItemType(safeDoc);

  const name = safeText(
    safeDoc?.chunkName,
    safeDoc?.name,
    safeDoc?.chunkID,
    "Tài liệu chưa đặt tên"
  );
  const desc = safeText(safeDoc?.chunkDescription, safeDoc?.description);
  const typeLabel = getTypeLabel(kind);
  const avatarLabel = getAvatarLabel(kind);
  const detailTo = getDetailTo(safeDoc);
  const viewTo = getViewTo(safeDoc);
  const resolvedFileUrl = getResolvedFileUrl(safeDoc);
  const saveCategory = getSaveCategory(safeDoc);

  const classId = safeText(safeDoc?.class?.classID);
  const subjectId = safeText(safeDoc?.subject?.subjectID);
  const topicId = safeText(safeDoc?.topic?.topicID);
  const lessonId = safeText(safeDoc?.lesson?.lessonID);

  const className = safeText(safeDoc?.class?.className, classId);
  const subjectName = safeText(safeDoc?.subject?.subjectName, subjectId);
  const topicName = safeText(safeDoc?.topic?.topicName, topicId);
  const lessonName = safeText(safeDoc?.lesson?.lessonName, lessonId);

  const classTo = kind === "chunk" && classId ? `/user/docs/${encodeURIComponent(classId)}?type=class` : null;
  const subjectTo = kind === "chunk" && subjectId ? `/user/docs/${encodeURIComponent(subjectId)}?type=subject` : null;
  const topicTo = kind === "chunk" && topicId ? `/user/docs/${encodeURIComponent(topicId)}?type=topic` : null;
  const lessonTo = kind === "chunk" && lessonId ? `/user/docs/${encodeURIComponent(lessonId)}?type=lesson` : null;

  const showOpen = kind !== "class" && Boolean(viewTo && resolvedFileUrl);
  const showDownload = kind !== "class" && Boolean(resolvedFileUrl);

  return (
    <article className="user-doc-card user-doc-card-clean">
      {detailTo ? (
        <Link className="user-doc-preview-link" to={detailTo}>
          <div className={`user-doc-file-avatar user-doc-file-avatar-${kind}`}>{avatarLabel}</div>
        </Link>
      ) : (
        <div className="user-doc-preview-link">
          <div className={`user-doc-file-avatar user-doc-file-avatar-${kind}`}>{avatarLabel}</div>
        </div>
      )}

      <div className="user-doc-card-main">
        <div className="user-doc-card-head compact">
          <div className="user-doc-card-title-wrap">
            {detailTo ? (
              <Link className="user-doc-card-title link" to={detailTo}>
                {name}
              </Link>
            ) : (
              <div className="user-doc-card-title">{name}</div>
            )}

            <div className="user-doc-card-badges">
              <span className="user-doc-badge">{typeLabel}</span>
              {safeDoc?.chunkID ? (
                <span className="user-doc-badge user-doc-badge-subtle">{safeDoc.chunkID}</span>
              ) : null}
            </div>
          </div>

          <div className="user-doc-card-actions user-doc-card-actions-desktop compact">
            {detailTo ? (
              <Link className="btn btn-primary" to={detailTo}>
                Chi tiết
              </Link>
            ) : null}

            {showOpen ? (
              <Link className="btn" to={viewTo}>
                Mở file
              </Link>
            ) : null}

            {showDownload ? (
              <a className="btn" href={resolvedFileUrl} target="_blank" rel="noreferrer">
                Tải tài liệu
              </a>
            ) : null}

            {typeof onToggleSave === "function" ? (
              <button
                className="btn"
                type="button"
                onClick={() => onToggleSave({ ...safeDoc, category: saveCategory })}
              >
                {safeDoc?.isSaved ? "Bỏ lưu" : "Lưu"}
              </button>
            ) : null}
          </div>
        </div>

        <div className="user-doc-card-structure user-doc-card-structure-compact">
          <MetaGroup title="Lớp" value={className} to={classTo} />
          <MetaGroup title="Môn" value={subjectName} to={subjectTo} />
          <MetaGroup title="Chủ đề" value={topicName} to={topicTo} />
          <MetaGroup title="Bài" value={lessonName} to={lessonTo} />
        </div>

        {desc ? <p className="user-doc-card-desc">{shortText(desc)}</p> : null}
        {kind !== "chunk" ? <div className="user-doc-card-desc">{safeText(safeDoc?.description)}</div> : null}

        <div className="user-doc-card-actions user-doc-card-actions-mobile compact">
          {detailTo ? (
            <Link className="btn btn-primary" to={detailTo}>
              Chi tiết
            </Link>
          ) : null}

          {showOpen ? (
            <Link className="btn" to={viewTo}>
              Mở file
            </Link>
          ) : null}

          {showDownload ? (
            <a className="btn" href={resolvedFileUrl} target="_blank" rel="noreferrer">
              Tải tài liệu
            </a>
          ) : null}

          {typeof onToggleSave === "function" ? (
            <button
              className="btn"
              type="button"
              onClick={() => onToggleSave({ ...safeDoc, category: saveCategory })}
            >
              {safeDoc?.isSaved ? "Bỏ lưu" : "Lưu"}
            </button>
          ) : null}
        </div>
      </div>
    </article>
  );
}