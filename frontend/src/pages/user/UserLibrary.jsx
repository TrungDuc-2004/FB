import { useEffect, useMemo, useState } from "react";
import "../../styles/admin/page.css";
import DocumentCard, { DocumentMappedItems } from "../../components/DocumentCard";
import {
  listClasses,
  listLessons,
  listChunks,
  listSubjects,
  listTopics,
  toggleSave,
} from "../../services/userDocsApi";
import {
  filterTinHocSubjects,
  pickTinHocSubject,
} from "../../utils/userDocHelpers";

function Select({ label, value, onChange, options, disabled }) {
  return (
    <label style={{ display: "grid", gap: 6 }}>
      <span style={{ fontSize: 13, color: "#475569", fontWeight: 600 }}>{label}</span>
      <select
        className="input"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled}
      >
        <option value="">Chọn {label.toLowerCase()}</option>
        {(options || []).map((item) => (
          <option key={item.value} value={item.value}>
            {item.label}
          </option>
        ))}
      </select>
    </label>
  );
}

function SortSelect({ value, onChange }) {
  return (
    <label style={{ display: "grid", gap: 6 }}>
      <span style={{ fontSize: 13, color: "#475569", fontWeight: 600 }}>Sắp xếp</span>
      <select className="input" value={value} onChange={(e) => onChange(e.target.value)}>
        <option value="name">Tên A → Z</option>
        <option value="newest">Mới nhất</option>
        <option value="oldest">Cũ nhất</option>
        <option value="updated">Cập nhật gần đây</option>
      </select>
    </label>
  );
}

export default function Library() {
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const [classes, setClasses] = useState([]);
  const [topics, setTopics] = useState([]);
  const [lessons, setLessons] = useState([]);
  const [chunks, setChunks] = useState([]);

  const [classID, setClassID] = useState("");
  const [subjectID, setSubjectID] = useState("");
  const [subjectLabel, setSubjectLabel] = useState("");
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

  const topicOptions = useMemo(
    () => (topics || []).map((t) => ({ value: t.topicID, label: t.topicName || t.topicID })),
    [topics]
  );

  const lessonOptions = useMemo(
    () => (lessons || []).map((l) => ({ value: l.lessonID, label: l.lessonName || l.lessonID })),
    [lessons]
  );

  const pathParts = useMemo(() => {
    const className = classes.find((x) => x.classID === classID)?.className || classID;
    const topicName = topics.find((x) => x.topicID === topicID)?.topicName || topicID;
    const lessonName = lessons.find((x) => x.lessonID === lessonID)?.lessonName || lessonID;
    return [className, subjectLabel, topicName, lessonName].filter(Boolean);
  }, [classID, classes, lessonID, lessons, subjectLabel, topicID, topics]);

  async function fetchTinHocSubject(classValue) {
    const res = await listSubjects({ classID: classValue });
    const items = filterTinHocSubjects(res.items || []);
    const selected = pickTinHocSubject(items);

    if (!selected) {
      setSubjectID("");
      setSubjectLabel("");
      return null;
    }

    setSubjectID(selected.subjectID);
    setSubjectLabel(selected.subjectName || selected.subjectID);
    return selected;
  }

  async function fetchTopics(subjectValue) {
    const res = await listTopics({ subjectID: subjectValue });
    const items = res.items || [];
    setTopics(items);
    return items;
  }

  async function fetchLessons(topicValue) {
    const res = await listLessons({ topicID: topicValue });
    const items = res.items || [];
    setLessons(items);
    return items;
  }

  async function fetchChunks(lessonValue, sortValue = sort) {
    const res = await listChunks({ lessonID: lessonValue, sort: sortValue });
    const items = res.items || [];
    setChunks(items);
    return items;
  }

  async function onChooseClass(value) {
    setClassID(value);
    setSubjectID("");
    setSubjectLabel("");
    setTopicID("");
    setLessonID("");
    setTopics([]);
    setLessons([]);
    setChunks([]);

    if (!value) return;

    try {
      setLoading(true);
      setErr("");
      const selectedSubject = await fetchTinHocSubject(value);
      if (!selectedSubject?.subjectID) return;
      await fetchTopics(selectedSubject.subjectID);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function onChooseTopic(value) {
    setTopicID(value);
    setLessonID("");
    setLessons([]);
    setChunks([]);

    if (!value) return;

    try {
      setLoading(true);
      setErr("");
      await fetchLessons(value);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function onChooseLesson(value, sortValue = sort) {
    setLessonID(value);
    setChunks([]);

    if (!value) return;

    try {
      setLoading(true);
      setErr("");
      await fetchChunks(value, sortValue);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function onChangeSort(nextSort) {
    setSort(nextSort);

    if (!lessonID) return;

    try {
      setLoading(true);
      setErr("");
      await fetchChunks(lessonID, nextSort);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function handleToggleSave(doc) {
    try {
      const res = await toggleSave(doc.chunkID, doc.chunkType || "document");
      setChunks((prev) =>
        prev.map((item) =>
          item.chunkID === doc.chunkID ? { ...item, isSaved: !!res.saved } : item
        )
      );
    } catch (e) {
      setErr(String(e?.message || e));
    }
  }

  return (
    <div className="page-shell">
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div className="page-title">Thư viện tài liệu Tin học</div>
          </div>
          <div className="breadcrumb">
            <div className="crumb">User</div>
            <div className="crumb active">Thư viện Tin học</div>
          </div>
        </div>

        <div
          className="page-header-bottom"
          style={{ gap: 10, marginTop: 10, color: "#64748b", fontSize: 13 }}
        >
          <span>• Chỉ hiển thị môn Tin học</span>
          <span>• Chọn theo lớp → chủ đề → bài</span>
          <span>• Nhấn tên file hoặc avatar để xem ngay</span>
        </div>
      </div>

      <section className="content-panel" style={{ display: "grid", gap: 16 }}>
        <div
          style={{
            display: "grid",
            gap: 16,
            gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          }}
        >
          <Select
            label="Lớp"
            value={classID}
            onChange={onChooseClass}
            options={classOptions}
            disabled={loading}
          />

          <label style={{ display: "grid", gap: 6 }}>
            <span style={{ fontSize: 13, color: "#475569", fontWeight: 600 }}>Môn</span>
            <input className="input" value={subjectLabel} disabled placeholder="Tự động chọn môn Tin học" />
          </label>

          <Select
            label="Chủ đề"
            value={topicID}
            onChange={onChooseTopic}
            options={topicOptions}
            disabled={!subjectID || loading}
          />

          <Select
            label="Bài"
            value={lessonID}
            onChange={(value) => onChooseLesson(value, sort)}
            options={lessonOptions}
            disabled={!topicID || loading}
          />

          <SortSelect value={sort} onChange={onChangeSort} />
        </div>

        {pathParts.length ? (
          <div style={{ fontSize: 14, color: "#334155", fontWeight: 600 }}>
            Đang xem: {pathParts.join(" / ")}
          </div>
        ) : null}

        {err ? <div className="user-doc-empty">{err}</div> : null}
        {loading ? <div className="user-doc-empty">Đang tải dữ liệu...</div> : null}

        {!loading && lessonID && !chunks.length ? (
          <div className="user-doc-empty">Không có tài liệu trong bài này.</div>
        ) : null}

        <div style={{ display: "grid", gap: 16 }}>
          {chunks.map((doc) => (
            <div key={doc.chunkID} className="user-doc-result-group">
            <DocumentCard doc={doc} onToggleSave={handleToggleSave} />
            <DocumentMappedItems doc={doc} />
          </div>
          ))}
        </div>
      </section>
    </div>
  );
}