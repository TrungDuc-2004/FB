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
  return {
    class_map: deriveClassMapFromSubjectMap(subject_map),
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
  return {
    class_map: deriveClassMapFromSubjectMap(subject_map),
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
  return {
    class_map: deriveClassMapFromSubjectMap(subject_map),
    subject_map,
    topic_map,
    lesson_map,
    chunk_map: s,
    topicNumber,
    lessonNumber,
    chunkNumber,
  };
}

function parseMediaMapID(mapID = "") {
  const s = String(mapID || "").trim();
  const m = s.match(/^(IMG|VD)_(.+)$/i);
  if (!m) return null;
  const prefix = m[1].toUpperCase();
  const followMap = m[2];
  const chunk = parseChunkMap(followMap);
  if (chunk) return { prefix, followType: "chunk", followMap, ...chunk };
  const lesson = parseLessonMap(followMap);
  if (lesson) return { prefix, followType: "lesson", followMap, ...lesson };
  const topic = parseTopicMap(followMap);
  if (topic) return { prefix, followType: "topic", followMap, ...topic };
  return {
    prefix,
    followType: "subject",
    followMap,
    class_map: deriveClassMapFromSubjectMap(followMap),
    subject_map: followMap,
    topic_map: "",
    lesson_map: "",
    chunk_map: "",
  };
}

function defaultMeta(folderType, category) {
  return {
    name: "",
    class_map: "",
    subject_map: "",
    topic_map: "",
    lesson_map: "",
    chunk_map: "",
    subjectName: "",
    subjectTitle: "",
    topicName: "",
    lessonName: "",
    lessonType: "",
    chunkName: "",
    chunkType: "",
    keywords: "",
    chunkDescription: "",
    topicNumber: "",
    lessonNumber: "",
    chunkNumber: "",
    category,
    folderType,
    mapID: "",
    imgDescription: "",
    videoDescription: "",
  };
}

function toDocumentPayload(meta) {
  const m = { ...meta };
  m.classMap = m.class_map;
  m.subjectMap = m.subject_map;
  m.topicMap = m.topic_map;
  m.lessonMap = m.lesson_map;
  m.chunkMap = m.chunk_map;
  m.classID = m.class_map;
  m.subjectID = m.subject_map;
  m.topicID = m.topic_map;
  m.lessonID = m.lesson_map;
  m.chunkID = m.chunk_map;
  return m;
}

function toMediaPayload(meta, category, file) {
  const name = String(meta.name || file?.name || "").trim();
  const out = {
    name,
    category,
    folderType: meta.folderType,
    mapID: String(meta.mapID || "").trim(),
  };
  if (category === "image") {
    out.imgName = name;
    out.imgDescription = String(meta.imgDescription || "").trim();
  } else {
    out.videoName = name;
    out.videoDescription = String(meta.videoDescription || "").trim();
  }
  return out;
}

export default function InsertMetadataModal({ open, onClose, folderName, onInsert }) {
  const folderType = useMemo(() => inferFolderType(folderName), [folderName]);
  const category = useMemo(() => inferCategory(folderName), [folderName]);
  const isMedia = category === "image" || category === "video";

  const [meta, setMeta] = useState(() => defaultMeta(folderType, category));
  const [file, setFile] = useState(null);

  useEffect(() => {
    if (!open) return;
    setMeta(defaultMeta(folderType, category));
    setFile(null);
  }, [open, folderType, category]);

  useEffect(() => {
    if (!open || isMedia) return;
    setMeta((prev) => {
      const next = { ...prev };
      if (folderType === "subject") {
        if (next.subject_map) next.class_map = next.class_map || deriveClassMapFromSubjectMap(next.subject_map);
        return next;
      }
      if (folderType === "topic") {
        const d = parseTopicMap(next.topic_map);
        return d ? { ...next, ...d } : next;
      }
      if (folderType === "lesson") {
        const d = parseLessonMap(next.lesson_map);
        return d ? { ...next, ...d } : next;
      }
      if (folderType === "chunk") {
        const d = parseChunkMap(next.chunk_map);
        if (!d) return next;
        if (!next.chunkType && next.lessonType) d.chunkType = next.lessonType;
        return { ...next, ...d };
      }
      return next;
    });
  }, [open, isMedia, folderType, meta.subject_map, meta.topic_map, meta.lesson_map, meta.chunk_map, meta.lessonType]);

  const mediaDerived = useMemo(() => {
    if (!isMedia || !meta.mapID) return null;
    return parseMediaMapID(meta.mapID);
  }, [isMedia, meta.mapID]);

  if (!open) return null;

  function change(k, v) {
    setMeta((prev) => ({ ...prev, [k]: v }));
  }

  function validateMedia() {
    const mapID = String(meta.mapID || "").trim();
    if (!mapID) {
      alert("Vui lòng nhập mapID");
      return false;
    }
    const parsed = parseMediaMapID(mapID);
    if (!parsed) {
      alert("mapID không đúng format");
      return false;
    }
    const expectedPrefix = category === "image" ? "IMG" : "VD";
    if (parsed.prefix !== expectedPrefix) {
      alert(`mapID phải bắt đầu bằng ${expectedPrefix}_`);
      return false;
    }
    if (parsed.followType !== folderType) {
      alert(`mapID đang là cấp ${parsed.followType}, nhưng thư mục hiện tại là ${folderType}`);
      return false;
    }
    if (!file && !String(meta.name || "").trim()) {
      alert("Vui lòng nhập tên file hoặc chọn file");
      return false;
    }
    return true;
  }

  function validateDocument() {
    const needMapKey =
      folderType === "chunk" ? "chunk_map" : folderType === "lesson" ? "lesson_map" : folderType === "topic" ? "topic_map" : "subject_map";
    if (!String(meta[needMapKey] || "").trim()) {
      alert(`Vui lòng nhập ${needMapKey}`);
      return false;
    }
    if (!file && !String(meta.name || "").trim()) {
      alert("Vui lòng nhập tên file (có đuôi) hoặc chọn file");
      return false;
    }
    if (folderType === "chunk" && !String(meta.lesson_map || "").trim()) {
      alert("chunk_map không đúng format. VD: TH10_CD1_B1_C1");
      return false;
    }
    return true;
  }

  function submit(e) {
    e.preventDefault();
    if (isMedia) {
      if (!validateMedia()) return;
      onInsert({ meta: toMediaPayload(meta, category, file), file });
      return;
    }
    if (!validateDocument()) return;
    if (folderType === "chunk" && !String(meta.keywords || "").trim()) {
      const ok = window.confirm("Bạn chưa nhập keywords. Vẫn tiếp tục insert?");
      if (!ok) return;
    }
    onInsert({ meta: toDocumentPayload(meta), file });
  }

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

  const mapKey = mapFieldByType[folderType] || "subject_map";
  const infoFields = infoFieldsByType[folderType] || [];

  const titleMap = {
    subject: isMedia ? `Upload ${category === "image" ? "Image" : "Video"} (Subject)` : "Insert (Subject)",
    topic: isMedia ? `Upload ${category === "image" ? "Image" : "Video"} (Topic)` : "Insert (Topic)",
    lesson: isMedia ? `Upload ${category === "image" ? "Image" : "Video"} (Lesson)` : "Insert (Lesson)",
    chunk: isMedia ? `Upload ${category === "image" ? "Image" : "Video"} (Chunk)` : "Insert (Chunk)",
  };

  const mapHintByType = {
    subject: category === "image" ? "IMG_TH10" : "VD_TH10",
    topic: category === "image" ? "IMG_TH10_CD1" : "VD_TH10_CD1",
    lesson: category === "image" ? "IMG_TH10_CD1_B1" : "VD_TH10_CD1_B1",
    chunk: category === "image" ? "IMG_TH10_CD1_B1_C1" : "VD_TH10_CD1_B1_C1",
  };

  const derivedRows = isMedia
    ? [
        { k: "class_map", v: mediaDerived?.class_map },
        { k: "subject_map", v: mediaDerived?.subject_map },
        { k: "topic_map", v: mediaDerived?.topic_map },
        { k: "lesson_map", v: mediaDerived?.lesson_map },
        { k: "chunk_map", v: mediaDerived?.chunk_map },
      ].filter((x) => x.v)
    : [
        { k: "class_map", v: meta.class_map },
        { k: "subject_map", v: meta.subject_map },
        { k: "topic_map", v: meta.topic_map },
        { k: "lesson_map", v: meta.lesson_map },
      ].filter((x) => x.v);

  function renderInput(key) {
    if (key === "keywords" || key === "chunkDescription" || key === "imgDescription" || key === "videoDescription") {
      return (
        <textarea
          id={key}
          value={meta[key] || ""}
          onChange={(e) => change(key, e.target.value)}
          placeholder={
            key === "keywords"
              ? "CPU, RAM, Mainboard"
              : key === "imgDescription"
                ? "Mô tả ảnh"
                : key === "videoDescription"
                  ? "Mô tả video"
                  : "Mô tả"
          }
          rows={4}
        />
      );
    }
    return (
      <input
        id={key}
        value={meta[key] || ""}
        onChange={(e) => change(key, e.target.value)}
        placeholder={
          key === mapKey
            ? (folderType === "subject" ? "TH10" : folderType === "topic" ? "TH10_CD1" : folderType === "lesson" ? "TH10_CD1_B1" : "TH10_CD1_B1_C1")
            : key === "name"
              ? "ten_file.pdf"
              : ""
        }
      />
    );
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal modal-wide" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="modal-title">{titleMap[folderType] || "Insert"}</h3>
          <p className="modal-subtitle">
            {isMedia
              ? `Nhập mapID đúng cấp ${folderType}. Ví dụ: ${mapHintByType[folderType]}`
              : folderType === "subject"
                ? "Chỉ cần nhập subject_map (VD TH10). Class sẽ suy ra (L10)."
                : folderType === "topic"
                  ? "Chỉ cần nhập topic_map (VD TH10_CD1)."
                  : folderType === "lesson"
                    ? "Chỉ cần nhập lesson_map (VD TH10_CD1_B1)."
                    : "Chỉ cần nhập chunk_map (VD TH10_CD1_B1_C1). Lesson sẽ suy ra (TH10_CD1_B1)."}
          </p>
          <button className="modal-close" onClick={onClose}>×</button>
        </div>

        <form className="modal-form" onSubmit={submit}>
          <div className="modal-body">
            <div className="modal-note" style={{ marginTop: 0 }}>
              <div><strong>Đang ở:</strong> {folderName || "(root)"}</div>
              <div><strong>Folder type:</strong> {folderType}</div>
              <div><strong>Category:</strong> {category}</div>
            </div>

            <div style={{ marginTop: 16, marginBottom: 8, fontWeight: 600, color: "#0f172a" }}>
              Mapping ID
            </div>

            <div className="form-grid">
              <div className="field">
                <label htmlFor={isMedia ? "mapID" : mapKey}>
                  {isMedia ? `mapID (VD: ${mapHintByType[folderType]})` : mapKey}
                </label>
                {isMedia ? (
                  <input
                    id="mapID"
                    value={meta.mapID}
                    onChange={(e) => change("mapID", e.target.value)}
                    placeholder={mapHintByType[folderType]}
                  />
                ) : (
                  renderInput(mapKey)
                )}
              </div>
            </div>

            {derivedRows.length > 0 && (
              <div className="modal-note" style={{ marginTop: 12 }}>
                <strong>Map suy ra:</strong>
                <div style={{ marginTop: 6 }}>
                  {derivedRows.map((r) => (
                    <div key={r.k} style={{ fontFamily: "monospace", fontSize: 12 }}>
                      {r.k}: {r.v}
                    </div>
                  ))}
                  {isMedia && mediaDerived?.followType ? (
                    <div style={{ fontFamily: "monospace", fontSize: 12 }}>follow_type: {mediaDerived.followType}</div>
                  ) : null}
                </div>
              </div>
            )}

            <div style={{ marginTop: 16, marginBottom: 8, fontWeight: 600, color: "#0f172a" }}>
              Thông tin
            </div>

            {isMedia ? (
              <>
                <div className="field">
                  <label htmlFor="name">{category === "image" ? "Tên ảnh" : "Tên video"}</label>
                  <input
                    id="name"
                    value={meta.name}
                    onChange={(e) => change("name", e.target.value)}
                    placeholder={category === "image" ? "so_do_cpu.png" : "gioi_thieu_cpu.mp4"}
                  />
                </div>
                <div className="field" style={{ marginTop: 16 }}>
                  <label htmlFor={category === "image" ? "imgDescription" : "videoDescription"}>
                    {category === "image" ? "Mô tả ảnh" : "Mô tả video"}
                  </label>
                  {renderInput(category === "image" ? "imgDescription" : "videoDescription")}
                </div>
              </>
            ) : (
              <>
                <div className="form-grid">
                  {infoFields.filter((k) => k !== "chunkDescription").map((k) => (
                    <div className="field" key={k}>
                      <label htmlFor={k}>{k}</label>
                      {renderInput(k)}
                    </div>
                  ))}
                </div>
                {infoFields.includes("chunkDescription") && (
                  <div className="field" style={{ marginTop: 16 }}>
                    <label htmlFor="chunkDescription">chunkDescription</label>
                    {renderInput("chunkDescription")}
                  </div>
                )}
                <div className="field" style={{ marginTop: 8 }}>
                  <label htmlFor="name">Tên file (nếu không chọn file)</label>
                  <input
                    id="name"
                    value={meta.name}
                    onChange={(e) => change("name", e.target.value)}
                    placeholder="ten_file.pdf"
                  />
                </div>
              </>
            )}

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
              <strong>Lưu ý:</strong>{" "}
              {isMedia
                ? "MongoDB sẽ là nguồn gốc metadata. Postgre chỉ lưu quan hệ media và Neo4j sẽ tự sync node group theo follow_type."
                : <>URL lưu vào Mongo sẽ dùng <code>MINIO_PUBLIC_BASE_URL</code> + <code>/&lt;bucket&gt;/&lt;object_key&gt;</code>.</>}
            </div>
          </div>

          <div className="modal-footer">
            <button className="btn" type="button" onClick={onClose}>Huỷ</button>
            <button className="btn btn-primary" type="submit">Insert</button>
          </div>
        </form>
      </div>
    </div>
  );
}
