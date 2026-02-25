import { useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import "../../styles/admin/page.css";
import DocumentCard from "../../components/DocumentCard";
import {
  listClasses,
  listSubjects,
  listTopics,
  listLessons,
  searchDocs,
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
        <option value="">-- Tất cả --</option>
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </label>
  );
}

export default function Search() {
  const location = useLocation();
  const navigate = useNavigate();

  const [q, setQ] = useState("");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const [classes, setClasses] = useState([]);
  const [subjects, setSubjects] = useState([]);
  const [topics, setTopics] = useState([]);
  const [lessons, setLessons] = useState([]);

  const [classID, setClassID] = useState("");
  const [subjectID, setSubjectID] = useState("");
  const [topicID, setTopicID] = useState("");
  const [lessonID, setLessonID] = useState("");

  const [result, setResult] = useState({ total: 0, items: [] });

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

  // URL -> state (hỗ trợ /user/search?q=...)
  useEffect(() => {
    const sp = new URLSearchParams(location.search || "");
    const urlQ = (sp.get("q") || "").trim();
    const urlClassID = (sp.get("classID") || "").trim();
    const urlSubjectID = (sp.get("subjectID") || "").trim();
    const urlTopicID = (sp.get("topicID") || "").trim();
    const urlLessonID = (sp.get("lessonID") || "").trim();

    // chỉ set khi khác để tránh loop
    if (urlQ !== q) setQ(urlQ);
    if (urlClassID !== classID) setClassID(urlClassID);
    if (urlSubjectID !== subjectID) setSubjectID(urlSubjectID);
    if (urlTopicID !== topicID) setTopicID(urlTopicID);
    if (urlLessonID !== lessonID) setLessonID(urlLessonID);

    // nếu có query trên URL thì auto search
    if (urlQ) {
      (async () => {
        try {
          setLoading(true);
          setErr("");
          const res = await searchDocs({
            q: urlQ,
            classID: urlClassID,
            subjectID: urlSubjectID,
            topicID: urlTopicID,
            lessonID: urlLessonID,
            limit: 50,
            offset: 0,
          });
          const items = res?.items || res?.results || [];
          const total = typeof res?.total === "number" ? res.total : items.length;
          setResult({ total, items });
        } catch (e) {
          setErr(String(e?.message || e));
          setResult({ total: 0, items: [] });
        } finally {
          setLoading(false);
        }
      })();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.search]);

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

  async function runSearch(params = {}) {
    const res = await searchDocs({
      q,
      classID,
      subjectID,
      topicID,
      lessonID,
      ...params,
    });

    const items = res?.items || res?.results || [];
    const total = typeof res?.total === "number" ? res.total : items.length;

    setResult({ total, items });
  }

  async function onSubmit(e) {
    e?.preventDefault?.();
    try {
      setLoading(true);
      setErr("");

      // giữ query lên URL để refresh vẫn còn
      const sp = new URLSearchParams();
      if (q?.trim()) sp.set("q", q.trim());
      if (classID) sp.set("classID", classID);
      if (subjectID) sp.set("subjectID", subjectID);
      if (topicID) sp.set("topicID", topicID);
      if (lessonID) sp.set("lessonID", lessonID);
      navigate({ pathname: "/user/search", search: sp.toString() ? `?${sp.toString()}` : "" }, { replace: true });

      await runSearch({ limit: 50, offset: 0 });
    } catch (e2) {
      setErr(String(e2?.message || e2));
      setResult({ total: 0, items: [] });
    } finally {
      setLoading(false);
    }
  }

  async function onToggleSave(doc) {
    try {
      // chỉ chunk mới có save
      if (!doc?.chunkID) return;
      const r = await toggleSave(doc.chunkID);
      setResult((prev) => ({
        ...prev,
        items: (prev.items || []).map((x) => (x.chunkID === doc.chunkID ? { ...x, isSaved: r.saved } : x)),
      }));
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  const shown = (result.items || []).length;
  const totalText = result.total > shown ? `${shown}/${result.total}` : `${shown}`;

  return (
    <div>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div className="page-title">Tìm kiếm</div>
          </div>
          <div className="breadcrumb">
            <div className="crumb">User</div>
            <div className="crumb">Tìm kiếm</div>
          </div>
        </div>

        <form className="page-header-bottom" onSubmit={onSubmit} style={{ gap: 12 }}>
          <div style={{ flex: 1, display: "flex", gap: 10 }}>
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Nhập câu truy vấn (vd: Thông tin, cấu trúc dữ liệu...)"
              style={{
                flex: 1,
                height: 42,
                border: "1px solid #e2e8f0",
                borderRadius: 12,
                padding: "0 12px",
                outline: "none",
              }}
            />
            <button className="btn btn-primary" type="submit" disabled={loading}>
              Tìm
            </button>
          </div>
        </form>

        <div className="page-header-bottom" style={{ gap: 14, marginTop: 10 }}>
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
            onChange={setLessonID}
            options={lessonOptions}
            disabled={loading || !topicID}
          />
        </div>
      </div>

      {err ? <div style={{ marginBottom: 12, color: "#b91c1c", fontWeight: 700 }}>Lỗi: {err}</div> : null}
      {loading ? <div style={{ color: "#475569" }}>Đang tải…</div> : null}

      <div style={{ margin: "10px 0", color: "#0f172a", fontWeight: 800 }}>Kết quả: {totalText}</div>

      {!loading && !err && result.total === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">🔎</div>
          <div>Chưa có kết quả. Hãy thử đổi từ khoá hoặc chọn bộ lọc khác.</div>
        </div>
      ) : null}

      <div style={{ display: "grid", gap: 12 }}>
        {(result.items || []).map((d, idx) => {
          // chunk: giữ UI cũ
          if ((d?.type || "chunk") === "chunk" || d?.chunkID) {
            return <DocumentCard key={d.chunkID || `${idx}`} doc={d} onToggleSave={onToggleSave} />;
          }

          // lesson/topic/subject: card đơn giản
          const typeLabel = d?.type === "lesson" ? "Bài học" : d?.type === "topic" ? "Chủ đề" : d?.type === "subject" ? "Môn" : d?.type;
          return (
            <div
              key={`${d?.type || "item"}-${d?.id || idx}`}
              style={{
                background: "#fff",
                border: "1px solid #e2e8f0",
                borderRadius: 14,
                padding: 14,
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: 12,
              }}
            >
              <div style={{ minWidth: 0 }}>
                <div style={{ fontWeight: 900, color: "#0f172a", marginBottom: 4, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                  {typeLabel}: {d?.name || d?.id}
                </div>
                <div style={{ color: "#64748b", fontSize: 13, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                  {d?.class?.className ? `Lớp ${d.class.className}` : ""}
                  {d?.subject?.subjectName ? ` • ${d.subject.subjectName}` : ""}
                  {d?.topic?.topicName ? ` • ${d.topic.topicName}` : ""}
                  {d?.lesson?.lessonName ? ` • ${d.lesson.lessonName}` : ""}
                </div>
              </div>

              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <div style={{ fontWeight: 900, color: "#0f172a" }}>{typeof d?.score === "number" ? d.score.toFixed(3) : ""}</div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
