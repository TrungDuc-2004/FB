// frontend/src/components/InsertMetadataModal.jsx
import { useEffect, useMemo, useState } from "react";
import "../styles/admin/modal.css";

function splitPath(p = "") {
  return (p || "").split("/").filter(Boolean);
}

function normalizeFolderType(x = "") {
  const s = String(x || "").trim().toLowerCase();
  if (s === "topics") return "topic";
  if (s === "lessons") return "lesson";
  if (s === "chunks") return "chunk";
  if (s === "subjects") return "subject";
  return s;
}

function inferFolderType(folderName = "") {
  const parts = splitPath(folderName);
  const last = normalizeFolderType(parts[parts.length - 1] || "");
  if (["subject", "topic", "lesson", "chunk"].includes(last)) return last;
  // fallback: nếu không rõ thì coi như subject (theo flow bạn đang dùng)
  return "subject";
}

function inferCategory(folderName = "") {
  const parts = splitPath(folderName);
  const bucket = (parts[0] || "").toLowerCase();
  if (bucket === "images") return "image";
  if (bucket === "video" || bucket === "videos") return "video";
  return "document";
}

function extractLastNumber(s = "") {
  const m = String(s || "").match(/\d+/g);
  return m && m.length ? m[m.length - 1] : "";
}

function deriveClassMapFromSubjectMap(subject_map = "") {
  const n = extractLastNumber(subject_map);
  return n ? `L${n}` : "";
}

function parseTopicMap(topic_map = "") {
  const s = String(topic_map || "").trim();
  const m = s.match(/^(.+?)_CD(\d+)$/i);
  if (!m) return null;
  const subject_map = m[1];
  const topicNumber = m[2];
  const class_map = deriveClassMapFromSubjectMap(subject_map);
  return {
    class_map,
    subject_map,
    topic_map: s,
    topicNumber,
  };
}

function parseLessonMap(lesson_map = "") {
  const s = String(lesson_map || "").trim();
  const m = s.match(/^(.+?)_CD(\d+)_B(\d+)$/i);
  if (!m) return null;
  const subject_map = m[1];
  const topicNumber = m[2];
  const lessonNumber = m[3];
  const topic_map = `${subject_map}_CD${topicNumber}`;
  const class_map = deriveClassMapFromSubjectMap(subject_map);
  return {
    class_map,
    subject_map,
    topic_map,
    lesson_map: s,
    topicNumber,
    lessonNumber,
  };
}

function parseChunkMap(chunk_map = "") {
  const s = String(chunk_map || "").trim();
  const m = s.match(/^(.+?)_CD(\d+)_B(\d+)_C(\d+)$/i);
  if (!m) return null;
  const subject_map = m[1];
  const topicNumber = m[2];
  const lessonNumber = m[3];
  const chunkNumber = m[4];
  const topic_map = `${subject_map}_CD${topicNumber}`;
  const lesson_map = `${topic_map}_B${lessonNumber}`;
  const class_map = deriveClassMapFromSubjectMap(subject_map);
  return {
    class_map,
    subject_map,
    topic_map,
    lesson_map,
    chunk_map: s,
    topicNumber,
    lessonNumber,
    chunkNumber,
  };
}

function defaultMeta(folderType, category) {
  return {
    // file
    name: "",

    // map ids
    class_map: "",
    subject_map: "",
    topic_map: "",
    lesson_map: "",
    chunk_map: "",

    // display/name fields (giảm tối đa)
    subjectName: "",
    subjectTitle: "",

    topicName: "",

    lessonName: "",
    lessonType: "",

    chunkName: "",
    chunkType: "",
    keywords: "",
    chunkDescription: "",

    // derived numbers (không bắt nhập)
    topicNumber: "",
    lessonNumber: "",
    chunkNumber: "",

    // category dựa theo bucket
    category,

    // hint
    folderType,
  };
}

function toPayload(meta) {
  // Gửi map chain đầy đủ cho backend (backend có thể tự derive, nhưng gửi đủ để không gãy)
  const m = { ...meta };

  // alias camelCase
  m.classMap = m.class_map;
  m.subjectMap = m.subject_map;
  m.topicMap = m.topic_map;
  m.lessonMap = m.lesson_map;
  m.chunkMap = m.chunk_map;

  // alias "ID" để Postgre sync dễ đọc
  m.classID = m.class_map;
  m.subjectID = m.subject_map;
  m.topicID = m.topic_map;
  m.lessonID = m.lesson_map;
  m.chunkID = m.chunk_map;

  return m;
}

export default function InsertMetadataModal({ open, onClose, folderName, onInsert }) {
  const folderType = useMemo(() => inferFolderType(folderName), [folderName]);
  const category = useMemo(() => inferCategory(folderName), [folderName]);

  const [meta, setMeta] = useState(() => defaultMeta(folderType, category));
  const [file, setFile] = useState(null);

  useEffect(() => {
    if (!open) return;
    setMeta(defaultMeta(folderType, category));
    setFile(null);
  }, [open, folderType, category]);

  // ====== Derive chain từ "map sâu nhất" (user chỉ cần nhập 1 ô) ======
  useEffect(() => {
    if (!open) return;

    setMeta((prev) => {
      const next = { ...prev };

      if (folderType === "subject") {
        if (next.subject_map) {
          next.class_map = next.class_map || deriveClassMapFromSubjectMap(next.subject_map);
        }
        return next;
      }

      if (folderType === "topic") {
        if (!next.topic_map) return next;
        const d = parseTopicMap(next.topic_map);
        if (!d) return next;
        return { ...next, ...d };
      }

      if (folderType === "lesson") {
        if (!next.lesson_map) return next;
        const d = parseLessonMap(next.lesson_map);
        if (!d) return next;
        return { ...next, ...d };
      }

      if (folderType === "chunk") {
        if (!next.chunk_map) return next;
        const d = parseChunkMap(next.chunk_map);
        if (!d) return next;

        // chunkType mặc định = lessonType nếu chưa có
        if (!next.chunkType && next.lessonType) d.chunkType = next.lessonType;

        return { ...next, ...d };
      }

      return next;
    });
  }, [open, folderType, meta.subject_map, meta.topic_map, meta.lesson_map, meta.chunk_map, meta.lessonType]);

  if (!open) return null;

  function change(k, v) {
    setMeta((prev) => ({ ...prev, [k]: v }));
  }

  function submit(e) {
    e.preventDefault();

    // validate map sâu nhất
    const needMapKey =
      folderType === "chunk" ? "chunk_map" : folderType === "lesson" ? "lesson_map" : folderType === "topic" ? "topic_map" : "subject_map";

    const mv = String(meta[needMapKey] || "").trim();
    if (!mv) {
      alert(`Vui lòng nhập ${needMapKey}`);
      return;
    }

    // validate file/name
    const hasName = String(meta.name || "").trim();
    if (!file && !hasName) {
      alert("Vui lòng nhập tên file (có đuôi) hoặc chọn file");
      return;
    }

    // chunk folder: khuyến nghị có keyword
    if (folderType === "chunk") {
      const kw = String(meta.keywords || "").trim();
      if (!kw) {
        const ok = window.confirm("Bạn chưa nhập keywords. Vẫn tiếp tục insert?");
        if (!ok) return;
      }

      // chunk bắt buộc phải có lesson_map (để Postgre biết cha trực tiếp)
      if (!String(meta.lesson_map || "").trim()) {
        alert("chunk_map không đúng format. VD: TH10_CD1_B1_C1");
        return;
      }
    }

    onInsert({ meta: toPayload(meta), file });
  }

  const titleMap = {
    subject: "Insert (Subject)",
    topic: "Insert (Topic)",
    lesson: "Insert (Lesson)",
    chunk: "Insert (Chunk)",
  };

  const subtitleMap = {
    subject: "Chỉ cần nhập subject_map (VD TH10). Class sẽ suy ra (L10).",
    topic: "Chỉ cần nhập topic_map (VD TH10_CD1).",
    lesson: "Chỉ cần nhập lesson_map (VD TH10_CD1_B1).",
    chunk: "Chỉ cần nhập chunk_map (VD TH10_CD1_B1_C1). Lesson sẽ suy ra (TH10_CD1_B1).",
  };

  // ====== Field sets (giảm tối đa theo yêu cầu) ======
  const mapFieldByType = {
    subject: "subject_map",
    topic: "topic_map",
    lesson: "lesson_map",
    chunk: "chunk_map",
  };

  const infoFieldsByType = {
    subject: ["subjectName", "subjectTitle"],
    topic: ["topicName"],
    lesson: ["lessonName", "lessonType"],
    chunk: ["chunkName", "chunkType", "keywords", "chunkDescription"],
  };

  const label = {
    subject_map: "subject_map (VD: TH10)",
    topic_map: "topic_map (VD: TH10_CD1)",
    lesson_map: "lesson_map (VD: TH10_CD1_B1)",
    chunk_map: "chunk_map (VD: TH10_CD1_B1_C1)",

    subjectName: "Subject Name (tên môn)",
    subjectTitle: "Subject Title",

    topicName: "Topic Name",

    lessonName: "Lesson Name",
    lessonType: "Lesson Type (tùy chọn)",

    chunkName: "Chunk Name",
    chunkType: "Chunk Type (VD: pdf/docx/video...)",
    keywords: "Keywords (phân tách bằng , ; xuống dòng)",
    chunkDescription: "Chunk Description",

    name: "Tên file (nếu không chọn file)",
  };

  const placeholder = {
    subject_map: "TH10",
    topic_map: "TH10_CD1",
    lesson_map: "TH10_CD1_B1",
    chunk_map: "TH10_CD1_B1_C1",

    subjectName: "Tin học",
    subjectTitle: "Tin học 10",

    topicName: "Chủ đề 1",

    lessonName: "Bài 1",
    lessonType: "theory / practice / ...",

    chunkName: "Phân tích đề thi...",
    chunkType: "docx / pdf / video",
    keywords: "keyword1, keyword2",
    chunkDescription: "Mô tả ngắn...",

    name: "vd: bai1.docx (phải có đuôi)",
  };

  function renderInput(k) {
    const commonProps = {
      id: k,
      value: meta[k] ?? "",
      onChange: (e) => change(k, e.target.value),
      placeholder: placeholder[k] || "",
    };

    if (k === "chunkDescription") {
      return <textarea {...commonProps} className="kv-input" rows={3} />;
    }

    return <input {...commonProps} />;
  }

  const mapKey = mapFieldByType[folderType] || "subject_map";
  const infoFields = infoFieldsByType[folderType] || [];

  // ====== show derived chain để user kiểm (readonly) ======
  const derivedRows = [
    { k: "class_map", v: meta.class_map },
    { k: "subject_map", v: meta.subject_map },
    { k: "topic_map", v: meta.topic_map },
    { k: "lesson_map", v: meta.lesson_map },
  ].filter((x) => x.v);

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal modal-wide" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="modal-title">{titleMap[folderType] || "Insert"}</h3>
          <p className="modal-subtitle">{subtitleMap[folderType] || ""}</p>
          <button className="modal-close" onClick={onClose}>
            ×
          </button>
        </div>

        {/* form phải là flex để body cuộn */}
        <form className="modal-form" onSubmit={submit}>
          <div className="modal-body">
            <div className="modal-note" style={{ marginTop: 0 }}>
              <div>
                <strong>Đang ở:</strong> {folderName || "(root)"}
              </div>
              <div>
                <strong>Folder type:</strong> {folderType}
              </div>
              <div>
                <strong>Category (from bucket):</strong> {category}
              </div>
            </div>

            {/* MAP (chỉ 1 ô nhập) */}
            <div style={{ marginTop: 16, marginBottom: 8, fontWeight: 600, color: "#0f172a" }}>
              Mapping ID
            </div>

            <div className="form-grid">
              <div className="field">
                <label htmlFor={mapKey}>{label[mapKey]}</label>
                {renderInput(mapKey)}
              </div>
            </div>

            {/* Derived chain (readonly) */}
            {derivedRows.length > 0 && (
              <div className="modal-note" style={{ marginTop: 12 }}>
                <strong>Map suy ra:</strong>
                <div style={{ marginTop: 6 }}>
                  {derivedRows.map((r) => (
                    <div key={r.k} style={{ fontFamily: "monospace", fontSize: 12 }}>
                      {r.k}: {r.v}
                    </div>
                  ))}
                  {folderType === "chunk" && meta.chunk_map ? (
                    <div style={{ fontFamily: "monospace", fontSize: 12 }}>
                      chunk_map: {meta.chunk_map}
                    </div>
                  ) : null}
                </div>
              </div>
            )}

            {/* INFO */}
            <div style={{ marginTop: 16, marginBottom: 8, fontWeight: 600, color: "#0f172a" }}>
              Thông tin
            </div>

            <div className="form-grid">
              {infoFields
                .filter((k) => k !== "chunkDescription")
                .map((k) => (
                  <div className="field" key={k}>
                    <label htmlFor={k}>{label[k]}</label>
                    {renderInput(k)}
                  </div>
                ))}
            </div>

            {infoFields.includes("chunkDescription") && (
              <div className="field" style={{ marginTop: 16 }}>
                <label htmlFor="chunkDescription">{label.chunkDescription}</label>
                {renderInput("chunkDescription")}
              </div>
            )}

            {/* File name + upload */}
            <div className="field" style={{ marginTop: 8 }}>
              <label htmlFor="name">{label.name}</label>
              <input
                id="name"
                value={meta.name}
                onChange={(e) => change("name", e.target.value)}
                placeholder={placeholder.name}
              />
              <div style={{ fontSize: 12, color: "rgba(15,23,42,0.6)", marginTop: 8 }}>
                Nếu bạn chọn file bên dưới thì có thể để trống tên.
              </div>
            </div>

            <div className="field">
              <label htmlFor="file">Chọn file (tuỳ chọn)</label>
              <input id="file" type="file" onChange={(e) => setFile(e.target.files?.[0] || null)} />
              {file && (
                <div className="file-info">
                  <strong>Đã chọn:</strong> {file.name} ({Math.round(file.size / 1024)} KB)
                </div>
              )}
            </div>

            <div className="modal-note">
              <strong>Lưu ý:</strong> URL lưu vào Mongo sẽ dùng <code>MINIO_PUBLIC_BASE_URL</code> + <code>/{"<bucket>"}/{"<object_key>"}</code>.
              Chunk sẽ lưu quan hệ <code>lessonID = lesson_map</code> để Postgre sync.
            </div>
          </div>

          <div className="modal-footer">
            <button className="btn" type="button" onClick={onClose}>
              Huỷ
            </button>
            <button className="btn btn-primary" type="submit">
              Insert
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
