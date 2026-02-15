// pages/admin/MongoDB.jsx
import { useEffect, useMemo, useState } from "react";
import "../../styles/admin/page.css";
import "../../styles/admin/modal.css";
import DataTable from "../../components/DataTable";
import * as mongoApi from "../../services/mongoAdminApi";

/** ===== Helpers ===== */
function docTitle(doc = {}) {
  return (
    doc.className || doc.class_name ||
    doc.subjectName || doc.subject_name ||
    doc.topicName || doc.topic_name ||
    doc.lessonName || doc.lesson_name ||
    doc.chunkName || doc.chunk_name ||
    doc.keywordName || doc.keyword_name ||
    doc.imageName || doc.image_name ||
    doc.videoName || doc.video_name ||
    doc.tableName || doc.table_name ||
    doc.username ||
    doc.name ||
    ""
  );
}


// parse value để bạn nhập [] / {} là thành array/object thật
function parseValue(v) {
  const s = String(v ?? "").trim();
  if (s === "") return "";

  if (s.startsWith("{") || s.startsWith("[")) {
    try {
      return JSON.parse(s);
    } catch {
      return s;
    }
  }

  if (s === "true") return true;
  if (s === "false") return false;
  if (s === "null") return null;

  if (/^-?\d+(\.\d+)?$/.test(s)) return Number(s);

  return s;
}

function defaultPairsForCollection(col) {
  // mặc định minio dùng bucket data-edu
  const minioPrefix = { k: "minio", v: '{"bucket":"data-edu","prefix":""}' };
  const minioFile = { k: "minio", v: '{"bucket":"data-edu","object_key":"","url":""}' };

  switch (col) {
    case "class":
      return [{ k: "class_name", v: "" }, minioPrefix];

    case "subject":
      return [
        { k: "class_id", v: "" },
        { k: "subject_name", v: "" },
        { k: "subject_type", v: "" },
        minioPrefix,
      ];

    case "topic":
      return [
        { k: "subject_id", v: "" },
        { k: "topic_num", v: "" },
        { k: "topic_name", v: "" },
        minioPrefix,
      ];

    case "lesson":
      return [
        { k: "topic_id", v: "" },
        { k: "lesson_num", v: "" },
        { k: "lesson_name", v: "" },
        { k: "lesson_type", v: "ly thuyet" },
        minioPrefix,
      ];

    case "chunk":
      return [
        { k: "lesson_id", v: "" },
        { k: "chunk_label", v: "1" },
        { k: "chunk_name", v: "" },
        { k: "chunk_des", v: "" },
        { k: "images", v: "[]" },
        { k: "tables", v: "[]" },
        minioPrefix,
      ];

    case "keyword":
      return [
        { k: "chunk_id", v: "" },
        { k: "keyword_name", v: "" },
        { k: "keyword_des", v: "" },
      ];

    case "image":
      return [
        { k: "chunk_id", v: "" },
        { k: "image_name", v: "" },
        { k: "image_url", v: "[]" },
        minioFile,
      ];

    case "video":
      return [
        { k: "chunk_id", v: "" },
        { k: "video_name", v: "" },
        { k: "video_url", v: "[]" },
        minioFile,
      ];

    case "table":
      return [
        { k: "chunk_id", v: "" },
        { k: "table_name", v: "" },
        { k: "table_url", v: "[]" },
        minioFile,
      ];

    case "user":
      return [
        { k: "username", v: "" },
        { k: "password", v: "" },
        { k: "user_role", v: "user" },
        { k: "is_active", v: "true" },
      ];

    default:
      return [{ k: "name", v: "" }, minioPrefix, { k: "is_deleted", v: "false" }];
  }
}

/** ===== Mini modal: Create/Rename Collection ===== */
function CollectionModal({ open, onClose, initialName = "", title, onSubmit }) {
  const [name, setName] = useState(initialName);

  useEffect(() => {
    setName(initialName || "");
  }, [initialName, open]);

  if (!open) return null;

  function submit(e) {
    e.preventDefault();
    const n = name.trim();
    if (!n) return;
    onSubmit(n);
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="modal-title">{title}</h3>
          <button className="modal-close" onClick={onClose}>
            ×
          </button>
        </div>

        <div className="modal-body">
          <form onSubmit={submit}>
            <div className="field">
              <label>Tên collection</label>
              <input value={name} onChange={(e) => setName(e.target.value)} autoFocus />
            </div>

            <div className="modal-note">
              <strong>Lưu ý:</strong> Nên dùng chữ/số/_/- (vd: demo, class, lesson_10).
            </div>
          </form>
        </div>

        <div className="modal-footer">
          <button className="btn" onClick={onClose}>
            Huỷ
          </button>
          <button className="btn btn-primary" onClick={submit}>
            Lưu
          </button>
        </div>
      </div>
    </div>
  );
}

/** ===== Modal: Create/Edit Document (fields động) ===== */
function DocumentModal({ open, onClose, title, initialDoc, onSave, collectionName }) {
  const [pairs, setPairs] = useState([]);

  useEffect(() => {
    if (!open) return;

    if (!initialDoc) {
      setPairs(defaultPairsForCollection(collectionName));
      return;
    }

    const fields = initialDoc.fields || [];
    setPairs(fields.length ? fields : defaultPairsForCollection(collectionName));
  }, [open, initialDoc, collectionName]);

  if (!open) return null;

  function change(i, key, value) {
    setPairs((prev) => prev.map((p, idx) => (idx === i ? { ...p, [key]: value } : p)));
  }

  function addRow() {
    setPairs((prev) => [...prev, { k: "", v: "" }]);
  }

  function removeRow(i) {
    setPairs((prev) => prev.filter((_, idx) => idx !== i));
  }

  function submit(e) {
    e.preventDefault();

    const obj = {};
    for (const p of pairs) {
      const k = (p.k || "").trim();
      if (!k) continue;
      obj[k] = parseValue(p.v);
    }

    onSave(obj);
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="modal-title">{title}</h3>
          <p className="modal-subtitle">
            Tip: nhập <code>[]</code>/<code>{"{}"}</code> để lưu array/object. Field{" "}
            <code>minio</code> nên là JSON object.
          </p>
          <button className="modal-close" onClick={onClose}>
            ×
          </button>
        </div>

        <div className="modal-body">
          <form onSubmit={submit}>
            <div style={{ display: "grid", gap: 10 }}>
              {pairs.map((p, i) => {
                const keyName = (p.k || "").trim();
                const isBoolField = keyName === "is_deleted" || keyName === "is_active";

                return (
                  <div
                    key={i}
                    style={{
                      display: "grid",
                      gridTemplateColumns: "1fr 1.4fr auto",
                      gap: 10,
                      alignItems: "center",
                    }}
                  >
                    <input
                      className="kv-input"
                      placeholder="field (vd: class_name, minio...)"
                      value={p.k}
                      onChange={(e) => change(i, "k", e.target.value)}
                    />

                    {isBoolField ? (
                      <select
                        className="kv-input"
                        value={String(p.v ?? "false")}
                        onChange={(e) => change(i, "v", e.target.value)}
                      >
                        <option value="false">false</option>
                        <option value="true">true</option>
                      </select>
                    ) : (
                      <input
                        className="kv-input"
                        placeholder='value'
                        value={p.v}
                        onChange={(e) => change(i, "v", e.target.value)}
                      />
                    )}

                    <button
                      type="button"
                      className="btn"
                      onClick={() => removeRow(i)}
                      title="Xoá field"
                      style={{ height: 38 }}
                    >
                      ✕
                    </button>
                  </div>
                );
              })}
            </div>

            <div style={{ marginTop: 12, display: "flex", gap: 8 }}>
              <button type="button" className="btn" onClick={addRow}>
                + Thêm field
              </button>
            </div>
          </form>
        </div>

        <div className="modal-footer">
          <button className="btn" onClick={onClose}>
            Huỷ
          </button>
          <button className="btn btn-primary" onClick={submit}>
            Lưu document
          </button>
        </div>
      </div>
    </div>
  );
}

/** ===== Modal: Bulk Import XLSX ===== */
function ImportXlsxModal({ open, onClose, onImported }) {
  const [file, setFile] = useState(null);
  const [sync, setSync] = useState(true);
  const [category, setCategory] = useState("document");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!open) {
      setFile(null);
      setSync(true);
      setCategory("document");
      setLoading(false);
      setResult(null);
      setError("");
    }
  }, [open]);

  async function runImport() {
    if (!file) {
      setError("Bạn chưa chọn file XLSX");
      return;
    }
    setLoading(true);
    setError("");
    setResult(null);
    try {
      const res = await mongoApi.importMetadataXlsx(file, { sync, category });
      setResult(res);
      await onImported?.(res);
    } catch (e) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  if (!open) return null;

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" style={{ width: 720 }} onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Import metadata (XLSX)</h3>
          <button className="modal-close" onClick={onClose}>
            ✕
          </button>
        </div>

        <div className="modal-body">
          <p style={{ marginTop: 0 }}>
            Import sẽ upsert vào MongoDB (classes/subjects/topics/lessons/chunks) và (tuỳ chọn) sync sang PostgreSQL + Neo4j.
          </p>

          <p style={{ marginTop: 0 }}>
            Template (map IDs, không cần ref):{" "}
            <a href="/templates/Metadata_MapID_Template.xlsx" target="_blank" rel="noreferrer">
              Metadata_MapID_Template.xlsx
            </a>
          </p>

          <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 12, flexWrap: "wrap" }}>
            <input
              type="file"
              accept=".xlsx,.xlsm"
              onChange={(e) => setFile(e.target.files?.[0] || null)}
            />

            <label style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <input type="checkbox" checked={sync} onChange={(e) => setSync(e.target.checked)} />
              Sync PostgreSQL + Neo4j
            </label>

            <label style={{ display: "flex", gap: 8, alignItems: "center" }}>
              Category
              <select value={category} onChange={(e) => setCategory(e.target.value)}>
                <option value="document">document</option>
                <option value="image">image</option>
                <option value="video">video</option>
              </select>
            </label>
          </div>

          {error ? (
            <div className="empty-state" style={{ marginBottom: 12 }}>
              <div className="empty-state-icon">⚠️</div>
              <p style={{ whiteSpace: "pre-wrap" }}>{error}</p>
            </div>
          ) : null}

          {result ? (
            <div className="empty-state" style={{ marginBottom: 12 }}>
              <div className="empty-state-icon">✅</div>
              <p style={{ whiteSpace: "pre-wrap" }}>
                Mongo: classes(+{result?.mongo?.classes?.inserted}/~{result?.mongo?.classes?.updated}),
                subjects(+{result?.mongo?.subjects?.inserted}/~{result?.mongo?.subjects?.updated}),
                topics(+{result?.mongo?.topics?.inserted}/~{result?.mongo?.topics?.updated}),
                lessons(+{result?.mongo?.lessons?.inserted}/~{result?.mongo?.lessons?.updated}),
                chunks(+{result?.mongo?.chunks?.inserted}/~{result?.mongo?.chunks?.updated})\n
                Sync: ok={result?.sync?.ok}, failed={result?.sync?.failed}\n
                Errors: {Array.isArray(result?.errors) ? result.errors.length : 0}
              </p>
            </div>
          ) : null}

          {result?.errors?.length ? (
            <details>
              <summary>Xem lỗi</summary>
              <pre style={{ maxHeight: 220, overflow: "auto" }}>{JSON.stringify(result.errors.slice(0, 50), null, 2)}</pre>
            </details>
          ) : null}
        </div>

        <div className="modal-footer">
          <button className="btn" onClick={onClose} disabled={loading}>
            Đóng
          </button>
          <button className="btn btn-primary" onClick={runImport} disabled={loading}>
            {loading ? "Đang import..." : "Import"}
          </button>
        </div>
      </div>
    </div>
  );
}

export default function MongoDB() {
  const [current, setCurrent] = useState(""); // "" = root collections
  const [currentDocId, setCurrentDocId] = useState(""); // doc detail
  const [q, setQ] = useState("");

  const [collections, setCollections] = useState([]);
  const [docs, setDocs] = useState([]);
  const [totalDocs, setTotalDocs] = useState(0);

  const [err, setErr] = useState("");

  const [isEditingDoc, setIsEditingDoc] = useState(false);
  const [detailPairs, setDetailPairs] = useState([]);

  const isRoot = current === "";
  const currentCollection = current;
  const isDocDetail = !!currentDocId;

  // modals
  const [openCreateCol, setOpenCreateCol] = useState(false);
  const [openRenameCol, setOpenRenameCol] = useState(false);
  const [renameTarget, setRenameTarget] = useState(null); // {name}

  const [openCreateDoc, setOpenCreateDoc] = useState(false);
  const [openEditDoc, setOpenEditDoc] = useState(false);
  const [editDocTarget, setEditDocTarget] = useState(null); // doc

  const [openImport, setOpenImport] = useState(false);

  async function reloadCollections() {
    setErr("");
    try {
      const cols = await mongoApi.listCollections();
      const rows = (cols || []).map((name) => ({ id: name, name }));
      setCollections(rows);
    } catch (e) {
      setErr(String(e?.message || e));
      setCollections([]);
    }
  }

  async function reloadDocs(collectionName) {
    if (!collectionName) return;
    setErr("");
    try {
      const data = await mongoApi.listDocuments(collectionName, 200, 0);
      setDocs(data.documents || []);
      setTotalDocs(data.total ?? (data.documents || []).length);
    } catch (e) {
      setErr(String(e?.message || e));
      setDocs([]);
      setTotalDocs(0);
    }
  }

  useEffect(() => {
    reloadCollections();
  }, []);

  useEffect(() => {
    if (!currentCollection) return;
    reloadDocs(currentCollection);
  }, [currentCollection]);

  const selectedDoc = useMemo(() => {
    if (!currentDocId) return null;
    return docs.find((d) => String(d._id) === String(currentDocId)) || null;
  }, [docs, currentDocId]);

  function formatVal(k, val) {
    if (val == null) return "";
    // nếu là field *_at thì format
    if (k.endsWith("_at")) {
      const d = new Date(val);
      if (!isNaN(d.getTime())) return d.toLocaleString("vi-VN", { hour12: false });
    }
    return typeof val === "string" ? val : JSON.stringify(val);
  }

  function buildPairsFromDoc(doc) {
    if (!doc) return [];

    const keys = Object.keys(doc).sort((a, b) => a.localeCompare(b));

    const LOCK_FIELDS = new Set([
      "_id",
      "created_at",
      "created_by",
      "updated_at",
      "updated_by",
      "deleted_at",
    ]);

    return keys.map((k) => ({
      id: k,
      k,
      v: formatVal(k, doc[k]),
      locked: LOCK_FIELDS.has(k),
    }));
  }

  useEffect(() => {
    if (!selectedDoc) {
      setDetailPairs([]);
      return;
    }
    if (isEditingDoc) return;
    setDetailPairs(buildPairsFromDoc(selectedDoc));
  }, [selectedDoc, isEditingDoc]);

  const headerTitle = useMemo(() => {
    if (isRoot) return "MongoDB";
    return currentCollection;
  }, [isRoot, currentCollection]);

  const breadcrumbParts = useMemo(() => {
    if (isRoot) return [];
    return ["mongo", currentCollection];
  }, [isRoot, currentCollection]);

  function goBack() {
    if (currentDocId) {
      setIsEditingDoc(false);
      setCurrentDocId("");
      setQ("");
      return;
    }
    setCurrent("");
    setQ("");
  }

  const collectionRows = useMemo(() => {
    const s = q.trim().toLowerCase();
    const list = !s ? collections : collections.filter((c) => c.name.toLowerCase().includes(s));
    return list.slice().sort((a, b) => a.name.localeCompare(b.name));
  }, [collections, q]);

  const docRows = useMemo(() => {
    const s = q.trim().toLowerCase();

    const list = docs.map((d) => {
      const minio = d?.minio || {};
      const title = docTitle(d);

      const displayMinio =
        d.chunkUrl ||
        d.lessonUrl ||
        d.topicUrl ||
        d.subjectUrl ||
        minio.url ||
        (minio.bucket && minio.object_key ? `${minio.bucket}/${minio.object_key}` : "") ||
        (minio.bucket && minio.prefix ? `${minio.bucket}/${minio.prefix}` : "") ||
        minio.object_key ||
        minio.prefix ||
        "";


      return {
        ...d,
        id: String(d._id),
        _title: title,
        _minio_display: displayMinio,
      };
    });

    const filtered = !s
      ? list
      : list.filter(
          (d) =>
            String(d._id || "").includes(s) ||
            String(d._title || "")
              .toLowerCase()
              .includes(s) ||
            String(d._minio_display || "")
              .toLowerCase()
              .includes(s)
        );

    return filtered.slice();
  }, [docs, q]);

  const collectionColumns = [
    {
      key: "name",
      label: "COLLECTION",
      render: (r) => (
        <div className="folder-cell">
          <div className="folder-left">
            <div className="folder-icon">🧺</div>
            <div className="folder-divider" />
            <div className="folder-name" title={r.name}>
              {r.name}
            </div>
          </div>
          <div className="folder-right">›</div>
        </div>
      ),
    },
  ];

  const docColumns = [
    {
      key: "_id",
      label: "OBJECTID",
      render: (r) => (
        <span className="crumb" title={String(r._id)}>
          {String(r._id).slice(0, 10)}…
        </span>
      ),
    },
    {
      key: "_title",
      label: "NAME",
      render: (r) => (
        <div className="file-cell">
          <div className="file-left">
            <div className="file-icon file-other">📄</div>
            <div className="file-divider" />
            <div className="file-name" title={r._title || ""}>
              {r._title || "(no name field)"}
            </div>
          </div>
        </div>
      ),
    },
    {
      key: "_minio_display",
      label: "MINIO",
      render: (r) => (
        <span title={r._minio_display || ""} style={{ whiteSpace: "nowrap" }}>
          {r._minio_display
            ? String(r._minio_display).slice(0, 48) +
              (String(r._minio_display).length > 48 ? "…" : "")
            : ""}
        </span>
      ),
    },
  ];

  const detailViewColumns = [
    { key: "k", label: "FIELD", render: (r) => <span className="crumb">{r.k}</span> },
    {
      key: "v",
      label: "VALUE",
      render: (r) => (
        <span
          title={r.v}
          style={{
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
            display: "block",
            maxWidth: 560,
          }}
        >
          {r.v}
        </span>
      ),
    },
  ];

  const detailEditColumns = [
    {
      key: "k",
      label: "FIELD",
      render: (r) => (
        <input
          className="kv-input"
          value={r.k}
          disabled={r.locked}
          onChange={(e) => changePair(r.id, "k", e.target.value)}
        />
      ),
    },
    {
      key: "v",
      label: "VALUE",
      render: (r) => {
        const keyName = (r.k || "").trim();
        const isBoolField = keyName === "is_deleted" || keyName === "is_active";

        if (isBoolField) {
          return (
            <select
              className="kv-input"
              value={String(r.v ?? "false")}
              disabled={r.locked} // nếu field locked thì disable luôn
              onChange={(e) => changePair(r.id, "v", e.target.value)}
            >
              <option value="false">false</option>
              <option value="true">true</option>
            </select>
          );
        }

        return (
          <input
            className="kv-input"
            value={r.v}
            disabled={r.locked} // lock cả value nếu cần
            onChange={(e) => changePair(r.id, "v", e.target.value)}
          />
        );
      },
    },
  ];

  function openCollection(row) {
    setCurrent(row.name);
    setCurrentDocId("");
    setIsEditingDoc(false);
    setQ("");
  }

  async function createCollection(name) {
    const n = name.trim();
    if (!n) return;

    try {
      await mongoApi.createCollection(n);
      setOpenCreateCol(false);
      await reloadCollections();
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  async function renameCollectionSubmit(newName) {
    const n = newName.trim();
    if (!renameTarget) return;
    if (!n) return;

    try {
      await mongoApi.renameCollection(renameTarget.name, n);
      setOpenRenameCol(false);
      setRenameTarget(null);

      setCurrent((cur) => (cur === renameTarget.name ? n : cur));
      await reloadCollections();
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  async function deleteCollection(row) {
    if (!confirm(`Xoá collection "${row.name}" và toàn bộ documents?`)) return;
    try {
      await mongoApi.deleteCollection(row.name);
      if (current === row.name) setCurrent("");
      setQ("");
      await reloadCollections();
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  function docToModalFields(doc) {
    const entries = Object.entries(doc || {}).filter(([k]) => k !== "_id");
    return entries.map(([k, v]) => ({
      k,
      v: typeof v === "string" ? v : JSON.stringify(v),
    }));
  }

  async function createDoc(dataObj) {
    try {
      await mongoApi.createDocument(currentCollection, dataObj);
      setOpenCreateDoc(false);
      await reloadDocs(currentCollection);
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  function openEditDocModal(row) {
    setEditDocTarget(row);
    setOpenEditDoc(true);
  }

  async function saveEditDoc(dataObj) {
    if (!editDocTarget) return;

    try {
      await mongoApi.updateDocument(currentCollection, String(editDocTarget._id), dataObj);
      setOpenEditDoc(false);
      setEditDocTarget(null);
      await reloadDocs(currentCollection);
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  async function deleteDoc(row) {
    if (!confirm(`Xoá document "${docTitle(row) || row._id}"?`)) return;
    try {
      await mongoApi.deleteDocument(currentCollection, String(row._id));
      await reloadDocs(currentCollection);
      setCurrentDocId((id) => (String(id) === String(row._id) ? "" : id));
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  function changePair(id, key, value) {
    setDetailPairs((prev) => prev.map((p) => (p.id === id ? { ...p, [key]: value } : p)));
  }

  function removePair(id) {
    setDetailPairs((prev) => prev.filter((p) => p.id !== id));
  }

  function addFieldRow() {
    setDetailPairs((prev) => [...prev, { id: `new-${Date.now()}`, k: "", v: "", locked: false }]);
  }

  function cancelEditDoc() {
    setIsEditingDoc(false);
    setDetailPairs(buildPairsFromDoc(selectedDoc));
  }

  async function updateDocFromDetail() {
    if (!selectedDoc) return;

    const patch = {};
    for (const p of detailPairs) {
      const k = (p.k || "").trim();
      if (!k || k === "_id") continue;
      patch[k] = parseValue(p.v);
    }

    try {
      await mongoApi.updateDocument(currentCollection, String(selectedDoc._id), patch);
      setIsEditingDoc(false);
      await reloadDocs(currentCollection);
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  async function deleteDocFromDetail() {
    if (!selectedDoc) return;
    if (!confirm(`Xoá document "${docTitle(selectedDoc) || selectedDoc._id}"?`)) return;
    try {
      await mongoApi.deleteDocument(currentCollection, String(selectedDoc._id));
      setCurrentDocId("");
      await reloadDocs(currentCollection);
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  return (
    <div>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <h2 className="page-title">{headerTitle}</h2>

            {!isRoot && (
              <button className="back-btn back-btn-right" onClick={goBack}>
                Back →
              </button>
            )}
          </div>

          {!isRoot && (
            <div className="breadcrumb">
              {breadcrumbParts.map((p, idx, arr) => (
                <span key={idx} className="crumb">
                  {p}
                  {idx < arr.length - 1 ? <span className="sep">/</span> : null}
                </span>
              ))}
            </div>
          )}
        </div>

        <div className="page-header-bottom">
          <div className="search-box">
            <input
              placeholder={isRoot ? "Tìm collection..." : "Tìm document (name/_id/minio)..."}
              value={q}
              onChange={(e) => setQ(e.target.value)}
            />
          </div>

          <div className="header-actions">
            <button className="btn" onClick={() => setOpenImport(true)}>
              Import
            </button>
            {isRoot ? (
              <button className="btn btn-primary" onClick={() => setOpenCreateCol(true)}>
                + Collection
              </button>
            ) : (
              <button className="btn btn-primary" onClick={() => setOpenCreateDoc(true)}>
                + Document
              </button>
            )}
          </div>
        </div>
      </div>

      {err ? (
        <div className="empty-state" style={{ marginBottom: 16 }}>
          <div className="empty-state-icon">⚠️</div>
          <p>{err}</p>
        </div>
      ) : null}

      <div className="table-wrapper">
        {isRoot ? (
          <DataTable
            columns={collectionColumns}
            rows={collectionRows}
            getRowClassName={() => "row-click"}
            onRowDoubleClick={(row) => openCollection(row)}
            renderActions={(row) => (
              <div className="table-actions" onDoubleClick={(e) => e.stopPropagation()}>
                <button
                  className="btn"
                  onClick={(e) => {
                    e.stopPropagation();
                    setRenameTarget({ name: row.name });
                    setOpenRenameCol(true);
                  }}
                >
                  Sửa
                </button>
                <button
                  className="btn"
                  onClick={(e) => {
                    e.stopPropagation();
                    deleteCollection(row);
                  }}
                >
                  Xoá
                </button>
              </div>
            )}
          />
        ) : isDocDetail ? (
          <>
            {!isEditingDoc ? (
              <>
                <DataTable columns={detailViewColumns} rows={detailPairs} renderActions={null} />
                <div className="detail-footer">
                  <button className="btn" onClick={deleteDocFromDetail}>
                    Xoá
                  </button>
                  <div className="spacer" />
                  <button className="btn btn-primary" onClick={() => setIsEditingDoc(true)}>
                    Sửa
                  </button>
                </div>
              </>
            ) : (
              <>
                <DataTable
                  columns={detailEditColumns}
                  rows={detailPairs}
                  renderActions={(row) =>
                    row.locked ? null : (
                      <div className="table-actions" onDoubleClick={(e) => e.stopPropagation()}>
                        <button className="btn" onClick={() => removePair(row.id)}>
                          ✕
                        </button>
                      </div>
                    )
                  }
                />
                <div className="detail-footer">
                  <button className="btn" onClick={addFieldRow}>
                    + Field
                  </button>
                  <div className="spacer" />
                  <button className="btn" onClick={cancelEditDoc}>
                    Huỷ bỏ
                  </button>
                  <button className="btn btn-primary" onClick={updateDocFromDetail}>
                    Cập nhật
                  </button>
                </div>
              </>
            )}
          </>
        ) : (
          <DataTable
            columns={docColumns}
            rows={docRows}
            getRowClassName={() => "row-click"}
            onRowDoubleClick={(row) => {
              setCurrentDocId(String(row._id));
              setIsEditingDoc(false);
            }}
            renderActions={(row) => (
              <div className="table-actions" onDoubleClick={(e) => e.stopPropagation()}>
                <button className="btn" onClick={() => openEditDocModal(row)}>
                  Sửa
                </button>
                <button className="btn" onClick={() => deleteDoc(row)}>
                  Xoá
                </button>
              </div>
            )}
          />
        )}
      </div>

      <CollectionModal
        open={openCreateCol}
        onClose={() => setOpenCreateCol(false)}
        title="Tạo collection mới"
        onSubmit={createCollection}
      />

      <CollectionModal
        open={openRenameCol}
        onClose={() => {
          setOpenRenameCol(false);
          setRenameTarget(null);
        }}
        title="Đổi tên collection"
        initialName={renameTarget?.name || ""}
        onSubmit={renameCollectionSubmit}
      />

      <DocumentModal
        open={openCreateDoc}
        onClose={() => setOpenCreateDoc(false)}
        title={`Tạo document mới (${currentCollection})`}
        initialDoc={null}
        onSave={createDoc}
        collectionName={currentCollection}
      />

      <DocumentModal
        open={openEditDoc}
        onClose={() => {
          setOpenEditDoc(false);
          setEditDocTarget(null);
        }}
        title={`Sửa document (${currentCollection})`}
        initialDoc={
          editDocTarget
            ? { _id: String(editDocTarget._id), fields: docToModalFields(editDocTarget) }
            : null
        }
        onSave={saveEditDoc}
        collectionName={currentCollection}
      />

      <ImportXlsxModal
        open={openImport}
        onClose={() => setOpenImport(false)}
        onImported={async () => {
          await reloadCollections();
          if (currentCollection) await reloadDocs(currentCollection);
        }}
      />
    </div>
  );
}
