import { useEffect, useMemo, useState } from "react";
import "../../styles/admin/page.css";
import DocumentCard from "../../components/DocumentCard";
import {
  listClasses,
  listSubjects,
  listTopics,
  listLessons,
  listChunks,
  toggleSave,
} from "../../services/userDocsApi";

function Select({ label, value, onChange, options, disabled }) {
  return (
    <label style={{ display: "flex", flexDirection: "column", gap: 6, minWidth: 220 }}>
      <div style={{ fontSize: 12, color: "#475569", fontWeight: 700 }}>{label}</div>
      <select
        value={value}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value)}
        style={{
          height: 38,
          border: "1px solid #e2e8f0",
          borderRadius: 10,
          padding: "0 10px",
          background: disabled ? "#f8fafc" : "#fff",
          color: "#0f172a",
          outline: "none",
        }}
      >
        <option value="">-- Chọn --</option>
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </label>
  );
}

export default function Library() {
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const [classes, setClasses] = useState([]);
  const [subjects, setSubjects] = useState([]);
  const [topics, setTopics] = useState([]);
  const [lessons, setLessons] = useState([]);
  const [chunks, setChunks] = useState([]);

  const [classID, setClassID] = useState("");
  const [subjectID, setSubjectID] = useState("");
  const [topicID, setTopicID] = useState("");
  const [lessonID, setLessonID] = useState("");
  const [sort, setSort] = useState("name");

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setErr("");
        const res = await listClasses();
        setClasses(res.items || []);
      } catch (e) {
        setErr(String(e?.message || e));
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  const classOptions = useMemo(
    () => (classes || []).map((c) => ({ value: c.classID, label: c.className || c.classID })),
    [classes]
  );

  const subjectOptions = useMemo(
    () => (subjects || []).map((s) => ({ value: s.subjectID, label: s.subjectName || s.subjectID })),
    [subjects]
  );

  const topicOptions = useMemo(
    () => (topics || []).map((t) => ({ value: t.topicID, label: t.topicName || t.topicID })),
    [topics]
  );

  const lessonOptions = useMemo(
    () => (lessons || []).map((l) => ({ value: l.lessonID, label: l.lessonName || l.lessonID })),
    [lessons]
  );

  async function onChooseClass(v) {
    setClassID(v);
    setSubjectID("");
    setTopicID("");
    setLessonID("");
    setSubjects([]);
    setTopics([]);
    setLessons([]);
    setChunks([]);
    if (!v) return;
    try {
      setLoading(true);
      setErr("");
      const res = await listSubjects({ classID: v });
      setSubjects(res.items || []);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function onChooseSubject(v) {
    setSubjectID(v);
    setTopicID("");
    setLessonID("");
    setTopics([]);
    setLessons([]);
    setChunks([]);
    if (!v) return;
    try {
      setLoading(true);
      setErr("");
      const res = await listTopics({ subjectID: v });
      setTopics(res.items || []);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function onChooseTopic(v) {
    setTopicID(v);
    setLessonID("");
    setLessons([]);
    setChunks([]);
    if (!v) return;
    try {
      setLoading(true);
      setErr("");
      const res = await listLessons({ topicID: v });
      setLessons(res.items || []);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function onChooseLesson(v, nextSort) {
    setLessonID(v);
    setChunks([]);
    if (!v) return;
    try {
      setLoading(true);
      setErr("");
      const res = await listChunks({ lessonID: v, limit: 200, offset: 0, sort: nextSort || sort });
      setChunks(res.items || []);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function onToggleSave(doc) {
    try {
      const r = await toggleSave(doc.chunkID);
      setChunks((prev) =>
        (prev || []).map((x) => (x.chunkID === doc.chunkID ? { ...x, isSaved: r.saved } : x))
      );
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  return (
    <div>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div className="page-title">Thư viện tài liệu</div>
          </div>
          <div className="breadcrumb">
            <div className="crumb">User</div>
            <div className="crumb">Thư viện</div>
          </div>
        </div>

        <div className="page-header-bottom" style={{ gap: 14 }}>
          <Select
            label="Sắp xếp"
            value={sort}
            onChange={(v) => { setSort(v); if (lessonID) onChooseLesson(lessonID, v); }}
            options={[{ value: "name", label: "Tên" }, { value: "updated", label: "Mới cập nhật" }]}
            disabled={loading}
          />
          <Select label="Lớp" value={classID} onChange={onChooseClass} options={classOptions} disabled={loading} />
          <Select
            label="Môn"
            value={subjectID}
            onChange={onChooseSubject}
            options={subjectOptions}
            disabled={loading || !classID}
          />
          <Select
            label="Chủ đề"
            value={topicID}
            onChange={onChooseTopic}
            options={topicOptions}
            disabled={loading || !subjectID}
          />
          <Select
            label="Bài học"
            value={lessonID}
            onChange={onChooseLesson}
            options={lessonOptions}
            disabled={loading || !topicID}
          />
        </div>
      </div>

      {err ? (
        <div style={{ marginBottom: 12, color: "#b91c1c", fontWeight: 700 }}>Lỗi: {err}</div>
      ) : null}

      {loading ? <div style={{ color: "#475569" }}>Đang tải…</div> : null}

      {!loading && lessonID && (chunks || []).length === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">📄</div>
          <div>Chưa có tài liệu trong bài học này.</div>
        </div>
      ) : null}

      <div style={{ display: "grid", gap: 12 }}>
        {(chunks || []).map((d) => (
          <DocumentCard key={d.chunkID} doc={d} onToggleSave={onToggleSave} />
        ))}
      </div>
    </div>
  );
}
