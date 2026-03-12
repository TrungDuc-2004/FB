import { useEffect, useMemo, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import "../../styles/admin/page.css";
import DocumentCard from "../../components/DocumentCard";
import {
  listChunks,
  listClasses,
  listLessons,
  listSubjects,
  listTopics,
  searchDocs,
  toggleSave,
} from "../../services/userDocsApi";

const MEDIA_OPTIONS = [
  { value: "all", label: "Tất cả loại" },
  { value: "document", label: "Tài liệu" },
  { value: "image", label: "Ảnh" },
  { value: "video", label: "Video" },
];

const RECENT_SEARCHES_KEY = "user_recent_searches_v1";
const MAX_RECENT = 5;

function Select({ label, value, onChange, options, disabled }) {
  return (
    <label style={{ display: "flex", flexDirection: "column", gap: 6, minWidth: 220 }}>
      <div style={{ fontSize: 12, color: "#475569", fontWeight: 700 }}>{label}</div>
      <select
        value={value}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value)}
        style={{
          height: 42,
          border: "1px solid #dbe2ea",
          borderRadius: 12,
          padding: "0 12px",
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

function normalizeSearchResponse(res) {
  const rawItems = Array.isArray(res?.items)
    ? res.items
    : Array.isArray(res?.results)
    ? res.results
    : [];

  const chunkItems = rawItems.filter(
    (x) => x && ((x.type || "chunk") === "chunk" || x.chunkID || x.id)
  );

  const items = chunkItems.length > 0 ? chunkItems : rawItems;
  const total = typeof res?.total === "number" ? res.total : items.length;
  return { total, items, rawCount: rawItems.length };
}

function normalizeBookType(v) {
  return String(v || "").trim();
}

function uniqueBy(items, getKey) {
  const out = [];
  const seen = new Set();
  for (const item of items || []) {
    const key = getKey(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}


function aggregateSearchItems(items) {
  const chunks = Array.isArray(items)
    ? items.filter((item) => (item?.itemType || item?.type || "chunk") === "chunk")
    : [];
  const out = [...chunks];
  const groups = new Map();

  function pushGroup(kind, id, name, seed, extra = {}) {
    const cleanId = String(id || "").trim();
    if (!cleanId) return;

    const key = `${kind}::${cleanId}`;
    const fallbackChunkUrl = kind === "class" ? "" : (extra.chunkUrl || seed?.chunkUrl || "");
    const fallbackDescription = extra.chunkDescription || "";

    if (!groups.has(key)) {
      groups.set(key, {
        itemType: kind,
        type: kind,
        category: kind,
        chunkID: cleanId,
        chunkName: name || cleanId,
        chunkType: extra.chunkType || kind,
        chunkUrl: fallbackChunkUrl,
        chunkDescription: fallbackDescription,
        score: typeof seed?.score === "number" ? seed.score : 0,
        isSaved: false,
        class: seed?.class || { classID: "", className: "" },
        subject: seed?.subject || { subjectID: "", subjectName: "", subjectUrl: "" },
        topic: seed?.topic || { topicID: "", topicName: "", topicUrl: "" },
        lesson: seed?.lesson || { lessonID: "", lessonName: "", lessonType: "", lessonUrl: "" },
        mappedDocuments: [],
      });
    }

    const current = groups.get(key);
    current.score = Math.max(current.score || 0, typeof seed?.score === "number" ? seed.score : 0);
    if (!current.chunkUrl && fallbackChunkUrl) current.chunkUrl = fallbackChunkUrl;
    if (!current.chunkDescription && fallbackDescription) current.chunkDescription = fallbackDescription;
    if (!current.chunkType && extra.chunkType) current.chunkType = extra.chunkType;

    if (seed?.chunkID && !(current.mappedDocuments || []).some((item) => item.chunkID === seed.chunkID)) {
      current.mappedDocuments.push(seed);
      if (!current.chunkUrl && kind !== "class" && seed?.chunkUrl) {
        current.chunkUrl = seed.chunkUrl;
      }
    }
  }

  for (const item of chunks) {
    pushGroup("class", item?.class?.classID, item?.class?.className || item?.class?.classID, item);
    pushGroup("subject", item?.subject?.subjectID, item?.subject?.subjectName || item?.subject?.subjectID, item, {
      chunkUrl: item?.subject?.subjectUrl || item?.chunkUrl || "",
      chunkDescription: item?.subject?.subjectDescription || "",
    });
    pushGroup("topic", item?.topic?.topicID, item?.topic?.topicName || item?.topic?.topicID, item, {
      chunkUrl: item?.topic?.topicUrl || item?.chunkUrl || "",
      chunkDescription: item?.topic?.topicDescription || "",
    });
    pushGroup("lesson", item?.lesson?.lessonID, item?.lesson?.lessonName || item?.lesson?.lessonID, item, {
      chunkUrl: item?.lesson?.lessonUrl || item?.chunkUrl || "",
      chunkDescription: item?.lesson?.lessonDescription || "",
      chunkType: item?.lesson?.lessonType || "",
    });

    for (const media of item?.images || []) {
      pushGroup("image", media?.id, media?.name || media?.id, item, {
        chunkUrl: media?.url || "",
        chunkDescription: media?.description || "",
      });
    }
    for (const media of item?.videos || []) {
      pushGroup("video", media?.id, media?.name || media?.id, item, {
        chunkUrl: media?.url || "",
        chunkDescription: media?.description || "",
      });
    }
  }

  return [...out, ...Array.from(groups.values())];
}

function typeRank(item) {
  const kind = item?.itemType || item?.type || "chunk";
  return { chunk: 0, lesson: 1, topic: 2, subject: 3, class: 4, image: 5, video: 6 }[kind] ?? 99;
}

function buildParams({ q = "", classID = "", bookType = "", mediaType = "all" } = {}) {
  const sp = new URLSearchParams();
  if (q.trim()) sp.set("q", q.trim());
  if (classID) sp.set("classID", classID);
  if (bookType) sp.set("bookType", bookType);
  if (mediaType && mediaType !== "all") sp.set("mediaType", mediaType);
  return sp;
}

function readRecentSearches() {
  try {
    const raw = localStorage.getItem(RECENT_SEARCHES_KEY);
    const parsed = JSON.parse(raw || "[]");
    return Array.isArray(parsed) ? parsed.filter(Boolean).slice(0, MAX_RECENT) : [];
  } catch {
    return [];
  }
}

function saveRecentSearch(keyword) {
  const text = String(keyword || "").trim();
  if (!text) return;

  const current = readRecentSearches();
  const next = [text, ...current.filter((x) => x !== text)].slice(0, MAX_RECENT);
  localStorage.setItem(RECENT_SEARCHES_KEY, JSON.stringify(next));
}

function removeRecentSearch(keyword) {
  const current = readRecentSearches();
  const next = current.filter((x) => x !== keyword);
  localStorage.setItem(RECENT_SEARCHES_KEY, JSON.stringify(next));
  return next;
}

function clearRecentSearches() {
  localStorage.removeItem(RECENT_SEARCHES_KEY);
}

export default function Search() {
  const location = useLocation();
  const navigate = useNavigate();

  const [q, setQ] = useState("");
  const [loading, setLoading] = useState(false);
  const [filterLoading, setFilterLoading] = useState(false);
  const [err, setErr] = useState("");

  const [classes, setClasses] = useState([]);
  const [bookTypes, setBookTypes] = useState([]);

  const [classID, setClassID] = useState("");
  const [bookType, setBookType] = useState("");
  const [mediaType, setMediaType] = useState("all");

  const [result, setResult] = useState({ total: 0, items: [], rawCount: 0 });

  const [recentSearches, setRecentSearches] = useState(() => readRecentSearches());
  const [showRecent, setShowRecent] = useState(false);

  const reqSeqRef = useRef(0);
  const hierarchyCacheRef = useRef(new Map());
  const recentBoxRef = useRef(null);

  const classOptions = useMemo(
    () => (classes || []).map((c) => ({ value: c.classID, label: c.className || c.classID })),
    [classes]
  );

  const bookTypeOptions = useMemo(
    () =>
      uniqueBy(
        (bookTypes || [])
          .map((t) => normalizeBookType(t))
          .filter(Boolean)
          .sort((a, b) => a.localeCompare(b, "vi"))
          .map((t) => ({ value: t, label: t })),
        (x) => x.value
      ),
    [bookTypes]
  );

  async function fetchHierarchy(classValue, categoryValue) {
    const cacheKey = `${classValue}::${categoryValue}`;
    if (hierarchyCacheRef.current.has(cacheKey)) {
      return hierarchyCacheRef.current.get(cacheKey);
    }

    const subjects =
      (await listSubjects({ classID: classValue, category: categoryValue })).items || [];

    const topicsNested = await Promise.all(
      subjects.map((subject) =>
        listTopics({ subjectID: subject.subjectID, category: categoryValue }).then(
          (r) => r.items || []
        )
      )
    );
    const topics = topicsNested.flat();

    const lessonsNested = await Promise.all(
      topics.map((topic) =>
        listLessons({ topicID: topic.topicID, category: categoryValue }).then(
          (r) => r.items || []
        )
      )
    );
    const lessons = lessonsNested.flat();

    const data = { subjects, topics, lessons };
    hierarchyCacheRef.current.set(cacheKey, data);
    return data;
  }

  async function loadBookTypes(classValue, mediaValue) {
    if (!classValue) {
      setBookTypes([]);
      return;
    }

    const categories = mediaValue === "all" ? ["document", "image", "video"] : [mediaValue];
    setFilterLoading(true);

    try {
      const hierarchies = await Promise.all(
        categories.map((cat) => fetchHierarchy(classValue, cat))
      );

      const lessonTypes = hierarchies
        .flatMap((h) => h.lessons || [])
        .map((lesson) => normalizeBookType(lesson.lessonType))
        .filter(Boolean);

      setBookTypes(uniqueBy(lessonTypes, (x) => x));
    } catch (e) {
      setErr(String(e?.message || e));
      setBookTypes([]);
    } finally {
      setFilterLoading(false);
    }
  }

  async function browseByFilters({ classID: classValue, bookType: bookTypeValue, mediaType: mediaValue }) {
    if (!classValue) return [];

    const categories = mediaValue === "all" ? ["document", "image", "video"] : [mediaValue];
    const hierarchies = await Promise.all(
      categories.map((cat) =>
        fetchHierarchy(classValue, cat).then((h) => ({ ...h, _category: cat }))
      )
    );

    const lessonTasks = [];
    for (const h of hierarchies) {
      for (const lesson of h.lessons || []) {
        const lessonType = normalizeBookType(lesson.lessonType);
        if (bookTypeValue && lessonType !== normalizeBookType(bookTypeValue)) continue;
        lessonTasks.push({ lessonID: lesson.lessonID, category: h._category });
      }
    }

    if (!lessonTasks.length) return [];

    const chunkResults = await Promise.all(
      lessonTasks.map((task) =>
        listChunks({
          lessonID: task.lessonID,
          category: task.category,
          limit: 200,
          offset: 0,
        }).then((r) => (r.items || []).map((item) => ({ ...item, category: task.category })))
      )
    );

    return uniqueBy(
      chunkResults.flat(),
      (item) => `${item.category || "document"}::${item.chunkID || item.id}`
    );
  }

  useEffect(() => {
    (async () => {
      try {
        const res = await listClasses({ category: "all" });
        setClasses(res.items || []);
      } catch (e) {
        setErr(String(e?.message || e));
      }
    })();
  }, []);

  useEffect(() => {
    const onClickOutside = (event) => {
      if (!recentBoxRef.current) return;
      if (!recentBoxRef.current.contains(event.target)) {
        setShowRecent(false);
      }
    };

    document.addEventListener("mousedown", onClickOutside);
    return () => document.removeEventListener("mousedown", onClickOutside);
  }, []);

  useEffect(() => {
    const sp = new URLSearchParams(location.search || "");
    const urlQ = (sp.get("q") || "").trim();
    const urlClassID = (sp.get("classID") || "").trim();
    const urlBookType = (sp.get("bookType") || "").trim();
    const urlMediaType = (sp.get("mediaType") || "all").trim() || "all";

    setQ(urlQ);
    setClassID(urlClassID);
    setBookType(urlBookType);
    setMediaType(urlMediaType);
  }, [location.search]);

  useEffect(() => {
    loadBookTypes(classID, mediaType);
  }, [classID, mediaType]);

  useEffect(() => {
    const sp = new URLSearchParams(location.search || "");
    const urlQ = (sp.get("q") || "").trim();
    const urlClassID = (sp.get("classID") || "").trim();
    const urlBookType = (sp.get("bookType") || "").trim();
    const urlMediaType = (sp.get("mediaType") || "all").trim() || "all";

    if (!urlQ && !urlClassID && !urlBookType && (!urlMediaType || urlMediaType === "all")) {
      setResult({ total: 0, items: [], rawCount: 0 });
      return;
    }

    const myReq = ++reqSeqRef.current;

    (async () => {
      try {
        setLoading(true);
        setErr("");
        let items = [];

        if (urlQ) {
          const categories = urlMediaType === "all" ? ["document", "image", "video"] : [urlMediaType];
          const parts = await Promise.all(
            categories.map((cat) =>
              searchDocs({
                q: urlQ,
                classID: urlClassID,
                category: cat,
                limit: 100,
                offset: 0,
              }).then((r) => {
                const normalized = normalizeSearchResponse(r);
                return normalized.items.map((item) => ({
                  ...item,
                  category: item.category || cat,
                }));
              })
            )
          );
          items = parts.flat();
        } else {
          items = await browseByFilters({
            classID: urlClassID,
            bookType: urlBookType,
            mediaType: urlMediaType,
          });
        }

        if (urlBookType) {
          items = items.filter(
            (item) =>
              normalizeBookType(item?.lesson?.lessonType) === normalizeBookType(urlBookType)
          );
        }

        if (urlQ) {
          items = aggregateSearchItems(items);
        }

        items = uniqueBy(
          items,
          (item) => `${item.category || "document"}::${item.chunkID || item.id}`
        );

        items.sort((a, b) => {
          const scoreA = typeof a?.score === "number" ? a.score : -1;
          const scoreB = typeof b?.score === "number" ? b.score : -1;
          if (scoreA !== scoreB) return scoreB - scoreA;
          const rankA = typeRank(a);
          const rankB = typeRank(b);
          if (rankA !== rankB) return rankA - rankB;
          return String(a?.chunkName || a?.name || "").localeCompare(
            String(b?.chunkName || b?.name || ""),
            "vi"
          );
        });

        if (myReq !== reqSeqRef.current) return;
        setResult({ total: items.length, items, rawCount: items.length });
      } catch (e) {
        if (myReq !== reqSeqRef.current) return;
        setErr(String(e?.message || e));
        setResult({ total: 0, items: [], rawCount: 0 });
      } finally {
        if (myReq === reqSeqRef.current) {
          setLoading(false);
        }
      }
    })();
  }, [location.search]);

  function onSubmit(e) {
    e?.preventDefault?.();

    const keyword = q.trim();
    if (keyword) {
      saveRecentSearch(keyword);
      setRecentSearches(readRecentSearches());
    }

    const sp = buildParams({ q, classID, bookType, mediaType });
    navigate(
      {
        pathname: "/user/search",
        search: sp.toString() ? `?${sp.toString()}` : "",
      },
      { replace: true }
    );
    setShowRecent(false);
  }

  function onClearFilters() {
    setQ("");
    setClassID("");
    setBookType("");
    setMediaType("all");
    setBookTypes([]);
    navigate("/user/search", { replace: true });
  }

  async function onToggleSave(doc) {
    try {
      if (!doc?.chunkID) return;
      const currentCategory = doc?.category || mediaType || "document";
      const r = await toggleSave(
        doc.chunkID,
        currentCategory === "all" ? "document" : currentCategory
      );

      setResult((prev) => ({
        ...prev,
        items: (prev.items || []).map((x) =>
          x.chunkID === doc.chunkID &&
          (x.category || "document") === (doc.category || "document")
            ? { ...x, isSaved: r.saved }
            : x
        ),
      }));
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  function onClickRecent(keyword) {
    setQ(keyword);
    saveRecentSearch(keyword);
    setRecentSearches(readRecentSearches());

    const sp = buildParams({ q: keyword, classID, bookType, mediaType });
    navigate(
      {
        pathname: "/user/search",
        search: sp.toString() ? `?${sp.toString()}` : "",
      },
      { replace: true }
    );
    setShowRecent(false);
  }

  function onRemoveRecent(keyword) {
    const next = removeRecentSearch(keyword);
    setRecentSearches(next);
  }

  function onClearRecentAll() {
    clearRecentSearches();
    setRecentSearches([]);
  }

  const shown = (result.items || []).length;

  return (
    <div>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div className="page-title">Tìm kiếm</div>
          </div>
          <div className="breadcrumb">
            <div className="crumb">User</div>
            <div className="crumb active">Tìm kiếm</div>
          </div>
        </div>

        <form
          className="page-header-bottom"
          onSubmit={onSubmit}
          style={{ gap: 12, alignItems: "flex-end", flexWrap: "wrap" }}
        >
          <div
            ref={recentBoxRef}
            style={{
              flex: "1 1 300px",
              display: "flex",
              flexDirection: "column",
              gap: 6,
              position: "relative",
            }}
          >
            <div style={{ fontSize: 12, color: "#475569", fontWeight: 700 }}>Từ khóa</div>

            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              onFocus={() => setShowRecent(true)}
              placeholder="Nhập nội dung cần tìm..."
              style={{
                height: 42,
                border: "1px solid #dbe2ea",
                borderRadius: 12,
                padding: "0 12px",
                outline: "none",
              }}
            />

            {showRecent && recentSearches.length > 0 ? (
              <div
                style={{
                  position: "absolute",
                  top: "calc(100% + 6px)",
                  left: 0,
                  right: 0,
                  background: "#fff",
                  border: "1px solid #dbe2ea",
                  borderRadius: 14,
                  boxShadow: "0 14px 30px rgba(15, 23, 42, 0.08)",
                  overflow: "hidden",
                  zIndex: 20,
                }}
              >
                <div
                  style={{
                    padding: "10px 12px",
                    borderBottom: "1px solid #eef2f7",
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    fontSize: 13,
                    fontWeight: 700,
                    color: "#475569",
                  }}
                >
                  <span>5 tìm kiếm gần nhất</span>
                  <button
                    type="button"
                    onClick={onClearRecentAll}
                    style={{
                      border: 0,
                      background: "transparent",
                      color: "#2563eb",
                      fontWeight: 700,
                      cursor: "pointer",
                    }}
                  >
                    Xóa tất cả
                  </button>
                </div>

                {recentSearches.map((item) => (
                  <div
                    key={item}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      gap: 8,
                      padding: "10px 12px",
                      borderBottom: "1px solid #f1f5f9",
                    }}
                  >
                    <button
                      type="button"
                      onClick={() => onClickRecent(item)}
                      style={{
                        flex: 1,
                        textAlign: "left",
                        border: 0,
                        background: "transparent",
                        cursor: "pointer",
                        color: "#0f172a",
                      }}
                    >
                      🕘 {item}
                    </button>

                    <button
                      type="button"
                      onClick={() => onRemoveRecent(item)}
                      style={{
                        border: 0,
                        background: "transparent",
                        color: "#94a3b8",
                        fontSize: 18,
                        lineHeight: 1,
                        cursor: "pointer",
                      }}
                      title="Xóa tìm kiếm này"
                    >
                      ×
                    </button>
                  </div>
                ))}
              </div>
            ) : null}
          </div>

          <Select
            label="Chọn lớp"
            value={classID}
            onChange={(value) => {
              setClassID(value);
              setBookType("");
            }}
            options={classOptions}
            disabled={loading || filterLoading}
          />

          <Select
            label="Chọn loại sách"
            value={bookType}
            onChange={setBookType}
            options={bookTypeOptions}
            disabled={loading || filterLoading || !classID}
          />

          <label style={{ display: "flex", flexDirection: "column", gap: 6, minWidth: 220 }}>
            <div style={{ fontSize: 12, color: "#475569", fontWeight: 700 }}>Chọn loại</div>
            <select
              value={mediaType}
              disabled={loading}
              onChange={(e) => {
                setMediaType(e.target.value);
                setBookType("");
              }}
              style={{
                height: 42,
                border: "1px solid #dbe2ea",
                borderRadius: 12,
                padding: "0 12px",
                background: "#fff",
                color: "#0f172a",
                outline: "none",
              }}
            >
              {MEDIA_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </label>

          <div className="user-search-actions">
            <button className="btn btn-primary" type="submit" disabled={loading || filterLoading}>
              Tìm
            </button>
            <button className="btn" type="button" onClick={onClearFilters} disabled={loading || filterLoading}>
              Xóa lọc
            </button>
          </div>
        </form>

        <div
          className="page-header-bottom"
          style={{ gap: 10, marginTop: 10, color: "#64748b", fontSize: 13 }}
        >
          <span>• Có lại menu Tìm kiếm ở sidebar</span>
          <span>• Lưu 5 tìm kiếm gần nhất</span>
          <span>• Nhấn vào lịch sử để tìm lại nhanh</span>
        </div>
      </div>

      {err ? <div style={{ marginBottom: 12, color: "#b91c1c", fontWeight: 700 }}>Lỗi: {err}</div> : null}
      {filterLoading ? <div style={{ color: "#475569", marginBottom: 8 }}>Đang nạp loại sách…</div> : null}
      {loading ? <div style={{ color: "#475569" }}>Đang tải kết quả…</div> : null}

      <div style={{ margin: "10px 0", color: "#0f172a", fontWeight: 800 }}>
        Kết quả: {shown}
        {classID || bookType || mediaType !== "all" ? (
          <span style={{ marginLeft: 8, color: "#64748b", fontWeight: 600 }}>
            {classID ? `Lớp ${classID}` : "Tất cả lớp"}
            {bookType ? ` • ${bookType}` : ""}
            {mediaType !== "all"
              ? ` • ${MEDIA_OPTIONS.find((x) => x.value === mediaType)?.label}`
              : ""}
          </span>
        ) : null}
      </div>

      {!loading && !err && shown === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">🔎</div>
          <div>
            {!q && !classID
              ? "Nhập từ khóa hoặc chọn lớp để bắt đầu tìm kiếm."
              : "Chưa có kết quả. Hãy thử đổi từ khóa hoặc chọn bộ lọc khác."}
          </div>
        </div>
      ) : null}

      <div style={{ display: "grid", gap: 12 }}>
        {(result.items || []).map((d, idx) => (
          <DocumentCard
            key={`${d.category || "document"}-${d.chunkID || d.id || idx}`}
            doc={d}
            onToggleSave={onToggleSave}
          />
        ))}
      </div>
    </div>
  );
}