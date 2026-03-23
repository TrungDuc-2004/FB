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

function normalizeText(value = "") {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function looksLikeRawId(value = "") {
  const text = safeText(value);
  if (!text) return false;
  return /^(TH\d{1,2}(?:[-_][A-Z0-9]+)+|[A-Z]{2,}\d*(?:[-_][A-Z0-9]+)+)$/i.test(text);
}

function extractSuffixNumber(value = "", key = "") {
  const text = String(value || "").toUpperCase();
  if (!text || !key) return "";
  const match = text.match(new RegExp(`(?:^|[_-])${key}(\\d+)`, "i"));
  return match?.[1] || "";
}

function humanizeChunkType(value = "") {
  const text = normalizeText(value);
  if (!text) return "—";
  if (text.includes("ly thuyet")) return "Lý thuyết";
  if (text.includes("bai tap")) return "Bài tập";
  if (text.includes("thuc hanh")) return "Thực hành";
  if (text === "lesson") return "Bài học";
  if (text === "topic") return "Chủ đề";
  if (text === "subject") return "Môn học";
  if (text === "class") return "Lớp";
  return safeText(value, "—");
}

function formatClassName(classInfo = {}) {
  const rawName = safeText(classInfo?.className);
  const rawId = safeText(classInfo?.classID);
  if (rawName && normalizeText(rawName) !== normalizeText(rawId) && !looksLikeRawId(rawName)) {
    return rawName;
  }
  const num = extractSuffixNumber(rawId || rawName, "TH");
  return num ? `Lớp ${num}` : safeText(rawName, rawId, "—");
}

function formatSubjectName(subjectInfo = {}) {
  const rawName = safeText(subjectInfo?.subjectName);
  const rawId = safeText(subjectInfo?.subjectID, rawName);

  if (rawName && normalizeText(rawName) !== normalizeText(rawId) && !looksLikeRawId(rawName)) {
    return rawName;
  }

  const match = String(rawId || "").toUpperCase().match(/^TH(\d{1,2})(?:-(UD|KHMT))?$/i);
  if (!match) return safeText(rawName, rawId, "—");

  const grade = match[1];
  const branch = (match[2] || "").toUpperCase();
  if (branch === "UD") return `Tin học ${grade} - Ứng dụng`;
  if (branch === "KHMT") return `Tin học ${grade} - Khoa học máy tính`;
  return `Tin học ${grade}`;
}

function formatTopicName(topicInfo = {}) {
  const rawName = safeText(topicInfo?.topicName);
  const rawId = safeText(topicInfo?.topicID, rawName);

  if (rawName && normalizeText(rawName) !== normalizeText(rawId) && !looksLikeRawId(rawName)) {
    return rawName;
  }

  const num = extractSuffixNumber(rawId || rawName, "CD") || extractSuffixNumber(rawId || rawName, "T");
  return num ? `Chủ đề ${num}` : safeText(rawName, rawId, "—");
}

function formatLessonName(lessonInfo = {}) {
  const rawName = safeText(lessonInfo?.lessonName);
  const rawId = safeText(lessonInfo?.lessonID, rawName);

  if (rawName && normalizeText(rawName) !== normalizeText(rawId) && !looksLikeRawId(rawName)) {
    return rawName;
  }

  const num = extractSuffixNumber(rawId || rawName, "B");
  return num ? `Bài ${num}` : safeText(rawName, rawId, "—");
}

function formatChunkName(doc = {}) {
  const rawName = safeText(doc?.chunkName);
  const rawId = safeText(doc?.chunkID, rawName);

  if (rawName && normalizeText(rawName) !== normalizeText(rawId) && !looksLikeRawId(rawName)) {
    return rawName;
  }

  const lessonLabel = formatLessonName(doc?.lesson || {});
  const chunkNum = extractSuffixNumber(rawId || rawName, "C");
  if (lessonLabel !== "—" && chunkNum) return `${lessonLabel} · Mục ${chunkNum}`;
  if (chunkNum) return `Mục ${chunkNum}`;
  return safeText(rawName, rawId, "Chi tiết tài liệu");
}

function getCurrentTitle(doc = {}, itemType = "document", fallbackId = "") {
  switch (itemType) {
    case "subject":
      return formatSubjectName({ subjectID: doc?.chunkID, subjectName: doc?.chunkName || doc?.subject?.subjectName });
    case "topic":
      return formatTopicName({ topicID: doc?.chunkID, topicName: doc?.chunkName || doc?.topic?.topicName });
    case "lesson":
      return formatLessonName({ lessonID: doc?.chunkID, lessonName: doc?.chunkName || doc?.lesson?.lessonName });
    case "class":
      return formatClassName({ classID: doc?.chunkID, className: doc?.chunkName || doc?.class?.className });
    case "chunk":
    case "document":
      return formatChunkName(doc);
    default:
      return safeText(doc?.chunkName, doc?.chunkID, fallbackId, "Chi tiết tài liệu");
  }
}

function getResolvedItemType(doc = {}, currentType = "document") {
  const raw = String(doc?.itemType || currentType || "document").toLowerCase();
  return raw === "document" ? "chunk" : raw;
}

function buildHierarchy(doc = {}, itemType = "chunk") {
  const items = [];
  const addItem = (kind, id, title, subtitle) => {
    const safeId = safeText(id);
    const safeTitle = safeText(title);
    if (!safeId || !safeTitle) return;
    items.push({ kind, id: safeId, title: safeTitle, subtitle });
  };

  if (itemType === "chunk") {
    addItem("lesson", doc?.lesson?.lessonID, formatLessonName(doc?.lesson), "Bài học");
  }

  if (itemType === "chunk" || itemType === "lesson") {
    addItem("topic", doc?.topic?.topicID, formatTopicName(doc?.topic), "Chủ đề");
  }

  if (itemType === "chunk" || itemType === "lesson" || itemType === "topic") {
    addItem("subject", doc?.subject?.subjectID, formatSubjectName(doc?.subject), "Môn học");
  }

  return items;
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
          url: safeText(item?.url),
          description: normalizeDescriptionText(item?.description),
          followType: safeText(item?.followType).toLowerCase(),
          followID: safeText(item?.followID),
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
          url: safeText(item?.url),
          description: normalizeDescriptionText(item?.description),
          followType: safeText(item?.followType).toLowerCase(),
          followID: safeText(item?.followID),
        }))
    : [];

  return [...images, ...videos];
}

function groupMediaByFollowType(items = []) {
  const order = ["chunk", "lesson", "topic", "subject"];
  const meta = {
    chunk: { title: "Media của mục này", empty: "Không có media gắn trực tiếp với mục này." },
    lesson: { title: "Media của bài học", empty: "Không có media ở cấp bài học." },
    topic: { title: "Media của chủ đề", empty: "Không có media ở cấp chủ đề." },
    subject: { title: "Media của môn học", empty: "Không có media ở cấp môn học." },
  };

  return order
    .map((followType) => ({
      followType,
      title: meta[followType].title,
      empty: meta[followType].empty,
      items: items.filter((item) => (item.followType || "") === followType),
    }))
    .filter((group) => group.items.length > 0);
}

function buildKeywords(doc = {}) {
  const rawKeywords = [
    ...(Array.isArray(doc?.keywords) ? doc.keywords : []),
    ...(Array.isArray(doc?.keywordItems)
      ? doc.keywordItems.map((item) => item?.keywordName || item?.keyword_name || item?.name || item?.keyword)
      : []),
  ];

  const seen = new Set();
  return rawKeywords
    .map((item) => safeText(item))
    .filter((item) => {
      if (!item) return false;
      const key = normalizeText(item);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

function buildInfoRows(doc = {}, itemType = "chunk") {
  const className = formatClassName(doc?.class);
  const subjectName = formatSubjectName(doc?.subject);
  const topicName = formatTopicName(doc?.topic);
  const lessonName = formatLessonName(doc?.lesson);
  const currentTitle = getCurrentTitle(doc, itemType, doc?.chunkID);

  if (itemType === "lesson") {
    return [
      { label: "Tên bài học", value: currentTitle },
      { label: "Bài", value: lessonName },
      { label: "Chủ đề", value: topicName },
      { label: "Sách", value: subjectName },
    ];
  }

  if (itemType === "topic") {
    return [
      { label: "Tên chủ đề", value: currentTitle },
      { label: "Chủ đề", value: topicName },
      { label: "Sách", value: subjectName },
      { label: "Lớp", value: className },
    ];
  }

  if (itemType === "subject") {
    return [
      { label: "Tên sách", value: currentTitle },
      { label: "Lớp", value: className },
    ];
  }

  if (itemType === "class") {
    return [{ label: "Lớp", value: currentTitle }];
  }

  return [
    { label: "Loại", value: humanizeChunkType(doc?.chunkType || itemType) },
    { label: "Lớp", value: className },
    { label: "Sách", value: subjectName },
    { label: "Chủ đề", value: topicName },
    { label: "Bài học", value: lessonName },
  ];
}

function MediaPreviewCard({ item }) {
  return (
    <Link className="doc-media-card" to={detailHref(item.id, item.kind)}>
      <div className="doc-media-thumb">
        {item.kind === "image" ? (
          <img src={item.url} alt={item.title} loading="lazy" />
        ) : (
          <video src={item.url} muted playsInline preload="metadata" />
        )}
        <span className={`doc-media-kind ${item.kind}`}>{item.kind === "image" ? "Ảnh" : "Video"}</span>
      </div>
      <div className="doc-media-body">
        <strong>{item.title}</strong>
        <span>{item.subtitle}</span>
        {item.description ? <p>{item.description}</p> : null}
      </div>
    </Link>
  );
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

  const itemType = getResolvedItemType(doc, currentType);
  const mappedFallbackUrl = Array.isArray(doc?.mappedDocuments) && ["chunk", "image", "video"].includes(itemType)
    ? doc.mappedDocuments.find((item) => item?.chunkUrl)?.chunkUrl || ""
    : "";

  const originalUrl = itemType === "class" ? "" : doc?.chunkUrl || mappedFallbackUrl || "";
  const viewUrl = view?.viewUrl || originalUrl;
  const ext = useMemo(() => getExt(viewUrl || originalUrl), [viewUrl, originalUrl]);
  const kindLabel = useMemo(() => getTypeLabel(doc?.itemType || currentType), [doc, currentType]);
  const title = getCurrentTitle(doc || {}, itemType, chunkID);
  const description = normalizeDescriptionText(
    doc?.chunkDescription,
    doc?.lesson?.lessonDescription,
    doc?.topic?.topicDescription,
    doc?.subject?.subjectDescription,
    "Nội dung chi tiết của tài liệu được hiển thị trong vùng xem ở giữa."
  );

  const canIframe = ["pdf"].includes(ext);
  const canImage = ["png", "jpg", "jpeg", "webp", "gif"].includes(ext);
  const canVideo = ["mp4", "webm", "ogg"].includes(ext);
  const isClass = itemType === "class";

  const hierarchyItems = buildHierarchy(doc || {}, itemType);
  const mediaItems = buildMedia(doc || {});
  const mediaGroups = groupMediaByFollowType(mediaItems);
  const keywords = buildKeywords(doc || {});
  const infoRows = buildInfoRows(doc || {}, itemType).filter((row) => safeText(row?.value) && row.value !== "—");
  const showKeywords = itemType === "chunk" && keywords.length > 0;
  const hasRelatedSection = hierarchyItems.length > 0 || mediaGroups.length > 0;

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
              {infoRows.map((row) => (
                <div key={row.label}>
                  <span>{row.label}</span>
                  <strong>{row.value}</strong>
                </div>
              ))}
              {itemType === "chunk" ? (
                <div>
                  <span>Keyword</span>
                  {showKeywords ? (
                    <div className="doc-keyword-list">
                      {keywords.map((keyword) => (
                        <span key={keyword} className="doc-keyword-chip">{keyword}</span>
                      ))}
                    </div>
                  ) : (
                    <strong>Chưa có keyword</strong>
                  )}
                </div>
              ) : null}
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
            <h3>Tài liệu liên quan</h3>
            {hasRelatedSection ? (
              <div className="doc-related-stack">
                {hierarchyItems.length ? (
                  <div className="doc-related-block">
                    <div className="doc-related-list hierarchy-only">
                      {hierarchyItems.map((item) => (
                        <Link key={`${item.kind}-${item.id}`} className="doc-related-item" to={detailHref(item.id, item.kind)}>
                          <strong>{item.title}</strong>
                          <span>{item.subtitle}</span>
                        </Link>
                      ))}
                    </div>
                  </div>
                ) : null}

                {mediaGroups.map((group) => (
                  <div key={group.followType} className="doc-related-block">
                    <div className="doc-related-subtitle">{group.title}</div>
                    <div className="doc-media-list">
                      {group.items.map((item) => (
                        <MediaPreviewCard key={`${item.kind}-${item.id}`} item={item} />
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="doc-related-empty">Chưa có mục liên quan để hiển thị.</div>
            )}
          </div>
        </aside>
      </div>
    </div>
  );
}
