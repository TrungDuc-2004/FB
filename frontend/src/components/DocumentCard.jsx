import { useState } from "react";
import { Link } from "react-router-dom";

function shortText(s, n = 120) {
  const t = (s || "").trim();
  if (!t) return "";
  return t.length > n ? t.slice(0, n - 1) + "…" : t;
}

function mediaTagLabel(kind) {
  if (kind === "chunk") return "chunk";
  if (kind === "lesson") return "bài";
  if (kind === "topic") return "chủ đề";
  if (kind === "subject") return "môn";
  return kind || "media";
}

function metaLine(doc) {
  const cls = doc?.class?.className || doc?.class?.classID || "";
  const subj = doc?.subject?.subjectName || doc?.subject?.subjectID || "";
  const topic = doc?.topic?.topicName || doc?.topic?.topicID || "";
  const lesson = doc?.lesson?.lessonName || doc?.lesson?.lessonID || "";
  const parts = [cls, subj, topic, lesson].filter(Boolean);
  return parts.join(" • ");
}

function MediaPanel({ title, items, icon }) {
  if (!items?.length) return null;
  return (
    <div
      style={{
        border: "1px solid #e2e8f0",
        borderRadius: 12,
        background: "#fff",
        padding: 10,
        minWidth: 0,
      }}
    >
      <div style={{ fontSize: 12.5, fontWeight: 800, color: "#0f172a", marginBottom: 8 }}>
        {icon} {title} ({items.length})
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {items.map((m) => (
          <a
            key={m.id || `${m.type}-${m.url}`}
            href={m.url || "#"}
            target="_blank"
            rel="noreferrer"
            title={m.description || m.name}
            style={{
              border: "1px solid #e2e8f0",
              borderRadius: 10,
              padding: "8px 10px",
              textDecoration: "none",
              color: "#334155",
              background: "#f8fafc",
            }}
          >
            <div style={{ fontSize: 13, fontWeight: 700, color: "#0f172a" }}>{m.name || m.id}</div>
            <div style={{ fontSize: 12, color: "#64748b", marginTop: 4 }}>Thuộc {mediaTagLabel(m.followType)}</div>
          </a>
        ))}
      </div>
    </div>
  );
}

export default function DocumentCard({ doc, onToggleSave }) {
  const [showMedia, setShowMedia] = useState(false);

  const name = doc?.chunkName || doc?.chunkID;
  const type = doc?.chunkType || doc?.lesson?.lessonType || "";
  const desc = doc?.chunkDescription || "";

  const lessonUrl = doc?.lesson?.lessonUrl || "";
  const topicUrl = doc?.topic?.topicUrl || "";
  const subjectUrl = doc?.subject?.subjectUrl || "";
  const images = Array.isArray(doc?.images) ? doc.images : [];
  const videos = Array.isArray(doc?.videos) ? doc.videos : [];
  const totalMedia = images.length + videos.length;

  return (
    <div
      style={{
        background: "#fff",
        border: "1px solid #e2e8f0",
        borderRadius: 14,
        padding: 14,
        boxShadow: "0 1px 2px rgba(15, 23, 42, 0.05)",
        display: "flex",
        gap: 12,
        alignItems: "flex-start",
      }}
    >
      <div
        style={{
          width: 44,
          height: 44,
          borderRadius: 12,
          background: "#f1f5f9",
          border: "1px solid #e2e8f0",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontWeight: 800,
          color: "#0f172a",
          flex: "0 0 auto",
        }}
        title={type || "Tài liệu"}
      >
        {String(type || "DOC").slice(0, 3).toUpperCase()}
      </div>

      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ display: "flex", gap: 10, alignItems: "baseline", flexWrap: "wrap" }}>
          <div style={{ fontSize: 15, fontWeight: 800, color: "#0f172a" }}>{name}</div>
          {doc?.chunkID ? <div style={{ fontSize: 12, color: "#64748b" }}>{doc.chunkID}</div> : null}
          {totalMedia ? (
            <div
              style={{
                fontSize: 12,
                color: "#0f172a",
                background: "#eef6ff",
                border: "1px solid #bfdbfe",
                borderRadius: 999,
                padding: "4px 8px",
                fontWeight: 700,
              }}
            >
              {images.length} ảnh • {videos.length} video
            </div>
          ) : null}
        </div>

        <div style={{ marginTop: 4, fontSize: 12.5, color: "#475569" }}>{metaLine(doc)}</div>

        {desc ? (
          <div style={{ marginTop: 8, fontSize: 13, color: "#334155", lineHeight: 1.5 }}>{shortText(desc)}</div>
        ) : null}

        <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
          <Link className="btn btn-primary" to={`/user/docs/${encodeURIComponent(doc.chunkID)}`}>
            Chi tiết
          </Link>
          <Link className="btn" to={`/user/view/${encodeURIComponent(doc.chunkID)}`}>
            Xem
          </Link>
          <button
            className="btn"
            type="button"
            onClick={() => onToggleSave?.(doc)}
            title={doc?.isSaved ? "Bỏ lưu" : "Lưu tài liệu"}
          >
            {doc?.isSaved ? "★ Đã lưu" : "☆ Lưu"}
          </button>

          {doc?.chunkUrl ? (
            <a className="btn" href={doc.chunkUrl} target="_blank" rel="noreferrer">
              Mở chunk
            </a>
          ) : null}

          {lessonUrl ? (
            <a className="btn" href={lessonUrl} target="_blank" rel="noreferrer" title="File của bài học">
              Mở bài
            </a>
          ) : null}
          {topicUrl ? (
            <a className="btn" href={topicUrl} target="_blank" rel="noreferrer" title="File của chủ đề">
              Mở chủ đề
            </a>
          ) : null}
          {subjectUrl ? (
            <a className="btn" href={subjectUrl} target="_blank" rel="noreferrer" title="File của môn">
              Mở môn
            </a>
          ) : null}

          {totalMedia ? (
            <button className="btn" type="button" onClick={() => setShowMedia((v) => !v)}>
              {showMedia ? "Ẩn media" : "Xem media"}
            </button>
          ) : null}
        </div>

        {showMedia && totalMedia ? (
          <div
            style={{
              marginTop: 12,
              paddingTop: 12,
              borderTop: "1px solid #e2e8f0",
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))",
              gap: 12,
            }}
          >
            <MediaPanel title="Ảnh liên quan" items={images} icon="🖼" />
            <MediaPanel title="Video liên quan" items={videos} icon="🎬" />
          </div>
        ) : null}
      </div>
    </div>
  );
}
