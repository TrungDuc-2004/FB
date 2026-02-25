import { Link } from "react-router-dom";

function shortText(s, n = 120) {
  const t = (s || "").trim();
  if (!t) return "";
  return t.length > n ? t.slice(0, n - 1) + "…" : t;
}

function metaLine(doc) {
  const cls = doc?.class?.className || doc?.class?.classID || "";
  const subj = doc?.subject?.subjectName || doc?.subject?.subjectID || "";
  const topic = doc?.topic?.topicName || doc?.topic?.topicID || "";
  const lesson = doc?.lesson?.lessonName || doc?.lesson?.lessonID || "";
  const parts = [cls, subj, topic, lesson].filter(Boolean);
  return parts.join(" • ");
}

export default function DocumentCard({ doc, onToggleSave }) {
  const name = doc?.chunkName || doc?.chunkID;
  const type = doc?.chunkType || doc?.lesson?.lessonType || "";
  const desc = doc?.chunkDescription || "";

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
          {doc?.chunkID ? (
            <div style={{ fontSize: 12, color: "#64748b" }}>{doc.chunkID}</div>
          ) : null}
        </div>
        <div style={{ marginTop: 4, fontSize: 12.5, color: "#475569" }}>{metaLine(doc)}</div>

        {desc ? (
          <div style={{ marginTop: 8, fontSize: 13, color: "#334155", lineHeight: 1.5 }}>
            {shortText(desc)}
          </div>
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
              Mở link
            </a>
          ) : null}
        </div>
      </div>
    </div>
  );
}
