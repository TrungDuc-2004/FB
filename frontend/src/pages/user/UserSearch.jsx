import { useEffect, useMemo, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import "../../styles/user/search.css";
import DocumentCard from "../../components/DocumentCard";
import { searchDocs, toggleSave } from "../../services/userDocsApi";

const MEDIA_OPTIONS = [
  { value: "all", label: "File type" },
  { value: "document", label: "Document" },
  { value: "image", label: "Image" },
  { value: "video", label: "Video" },
];

function FilterSelect({ value, onChange, options, allLabel = "All", disabled }) {
  return (
    <label className="search-filter-select">
      <select value={value} onChange={(event) => onChange(event.target.value)} disabled={disabled}>
        <option value="">{allLabel}</option>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
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

function normalizeBookType(value) {
  return String(value || "").trim();
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
    const fallbackChunkUrl = extra.chunkUrl || seed?.chunkUrl || "";
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
        images: seed?.images || [],
        videos: seed?.videos || [],
      });
    }

    const current = groups.get(key);
    current.score = Math.max(current.score || 0, typeof seed?.score === "number" ? seed.score : 0);
    if (!current.chunkUrl && fallbackChunkUrl) current.chunkUrl = fallbackChunkUrl;
    if (!current.chunkDescription && fallbackDescription) current.chunkDescription = fallbackDescription;
    if (!current.chunkType && extra.chunkType) current.chunkType = extra.chunkType;

    if (seed?.chunkID && !(current.mappedDocuments || []).some((item) => item.chunkID === seed.chunkID)) {
      current.mappedDocuments.push(seed);
      if (!current.chunkUrl && seed?.chunkUrl) current.chunkUrl = seed.chunkUrl;
    }
  }

  for (const item of chunks) {
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
  return { chunk: 0, lesson: 1, topic: 2, subject: 3, image: 4, video: 5 }[kind] ?? 99;
}

function buildParams({ q = "", bookType = "", mediaType = "all" } = {}) {
  const sp = new URLSearchParams();
  if (String(q || "").trim()) sp.set("q", String(q).trim());
  if (bookType) sp.set("bookType", bookType);
  if (mediaType && mediaType !== "all") sp.set("mediaType", mediaType);
  return sp;
}

function prettyCount(value) {
  return new Intl.NumberFormat("vi-VN").format(Number(value || 0));
}

function SearchSkeleton({ count = 4 }) {
  return (
    <div className="search-results-list">
      {Array.from({ length: count }).map((_, index) => (
        <div key={index} className="search-result-skeleton">
          <div className="search-result-skeleton-thumb" />
          <div className="search-result-skeleton-lines">
            <span className="w-20" />
            <span className="w-65" />
            <span className="w-92" />
            <span className="w-55" />
          </div>
        </div>
      ))}
    </div>
  );
}

export default function Search() {
  const location = useLocation();
  const navigate = useNavigate();

  const [q, setQ] = useState("");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const [bookType, setBookType] = useState("");
  const [mediaType, setMediaType] = useState("all");
  const [rawItems, setRawItems] = useState([]);

  const reqSeqRef = useRef(0);

  const bookTypeOptions = useMemo(
    () =>
      uniqueBy(
        rawItems
          .filter((item) => (item?.itemType || item?.type || "chunk") === "chunk")
          .map((item) => normalizeBookType(item?.chunkType))
          .filter(Boolean)
          .sort((a, b) => a.localeCompare(b, "vi"))
          .map((item) => ({ value: item, label: item })),
        (item) => item.value
      ),
    [rawItems]
  );

  const displayedItems = useMemo(() => {
    if (!bookType) return rawItems;
    return rawItems.filter((item) => {
      const kind = item?.itemType || item?.type || "chunk";
      if (kind !== "chunk") return true;
      return normalizeBookType(item?.chunkType) === bookType;
    });
  }, [rawItems, bookType]);

  useEffect(() => {
    const sp = new URLSearchParams(location.search || "");
    setQ((sp.get("q") || "").trim());
    setBookType((sp.get("bookType") || "").trim());
    setMediaType((sp.get("mediaType") || "all").trim() || "all");
  }, [location.search]);

  useEffect(() => {
    if (!q) {
      setErr("");
      setRawItems([]);
      return;
    }

    const seq = ++reqSeqRef.current;

    (async () => {
      try {
        setLoading(true);
        setErr("");

        const categories = mediaType === "all" ? ["document", "image", "video"] : [mediaType];
        const responses = await Promise.all(
          categories.map((category) =>
            searchDocs({ q, category, limit: 100, offset: 0 }).then((response) => {
              const normalized = normalizeSearchResponse(response);
              return (normalized.items || []).map((item) => ({
                ...item,
                category: item.category || category,
              }));
            })
          )
        );

        const items = aggregateSearchItems(responses.flat())
          .sort((a, b) => {
            const rankDiff = typeRank(a) - typeRank(b);
            if (rankDiff !== 0) return rankDiff;
            const scoreDiff = Number(b?.score || 0) - Number(a?.score || 0);
            if (scoreDiff !== 0) return scoreDiff;
            return String(a?.chunkName || "").localeCompare(String(b?.chunkName || ""), "vi");
          });

        if (seq !== reqSeqRef.current) return;
        setRawItems(uniqueBy(items, (item) => `${item.category || "document"}::${item.chunkID || item.id}`));
      } catch (error) {
        if (seq !== reqSeqRef.current) return;
        setErr(String(error?.message || error));
        setRawItems([]);
      } finally {
        if (seq === reqSeqRef.current) setLoading(false);
      }
    })();
  }, [q, mediaType]);

  useEffect(() => {
    if (!bookType) return;
    const exists = bookTypeOptions.some((option) => option.value === bookType);
    if (!exists) {
      const sp = buildParams({ q, mediaType, bookType: "" });
      navigate(`/user/search${sp.toString() ? `?${sp.toString()}` : ""}`, { replace: true });
    }
  }, [bookType, bookTypeOptions, mediaType, navigate, q]);

  function updateSearch(next = {}) {
    const sp = buildParams({
      q,
      bookType: typeof next.bookType === "string" ? next.bookType : bookType,
      mediaType: typeof next.mediaType === "string" ? next.mediaType : mediaType,
    });

    navigate(`/user/search${sp.toString() ? `?${sp.toString()}` : ""}`, { replace: true });
  }

  function handleBookTypeChange(value) {
    updateSearch({ bookType: value });
  }

  function handleMediaTypeChange(value) {
    updateSearch({ mediaType: value, bookType: "" });
  }

  function onClearFilters() {
    updateSearch({ bookType: "", mediaType: "all" });
  }

  async function onToggleSave(doc) {
    try {
      if (!doc?.chunkID) return;
      const currentCategory = doc?.category || mediaType || "document";
      const response = await toggleSave(doc.chunkID, currentCategory === "all" ? "document" : currentCategory);

      setRawItems((prev) =>
        (prev || []).map((item) =>
          item.chunkID === doc.chunkID && (item.category || "document") === (doc.category || "document")
            ? { ...item, isSaved: response.saved }
            : item
        )
      );
    } catch (error) {
      alert(String(error?.message || error));
    }
  }

  const shown = displayedItems.length;
  const hasSearchContext = Boolean(q);

  return (
    <div className="search-page-shell">
      <div className="search-filter-bar">
        <FilterSelect
          value={bookType}
          onChange={handleBookTypeChange}
          options={bookTypeOptions}
          allLabel="Loại sách"
          disabled={loading || !q || !bookTypeOptions.length}
        />

        <FilterSelect
          value={mediaType}
          onChange={handleMediaTypeChange}
          options={MEDIA_OPTIONS}
          allLabel="File type"
          disabled={loading || !q}
        />

        <button className="search-filter-clear" type="button" onClick={onClearFilters} disabled={loading || !q}>
          Clear all
        </button>
      </div>

      <div className="search-results-header">
        <div>
          <div className="search-results-title">{q ? `Kết quả cho “${q}”` : "Kết quả tìm kiếm"}</div>
          <div className="search-results-subtitle">
            {shown > 0 ? `${prettyCount(shown)} mục phù hợp` : "Dùng ô tìm kiếm ở thanh trên để bắt đầu."}
          </div>
        </div>
      </div>

      {err ? <div className="search-inline-message danger">Lỗi: {err}</div> : null}
      {loading ? <SearchSkeleton count={4} /> : null}

      {!loading && !err && !hasSearchContext ? (
        <div className="search-empty-state">Nhập từ khóa ở ô tìm kiếm phía trên để bắt đầu tìm kiếm.</div>
      ) : null}

      {!loading && !err && hasSearchContext && shown === 0 ? (
        <div className="search-empty-state">Không có kết quả phù hợp. Hãy thử đổi từ khóa hoặc nới bộ lọc.</div>
      ) : null}

      {!loading && !err && shown > 0 ? (
        <div className="search-results-list">
          {displayedItems.map((item, index) => (
            <DocumentCard
              key={`${item.category || "document"}-${item.chunkID || item.id || index}`}
              doc={item}
              onToggleSave={onToggleSave}
              variant="search-list"
            />
          ))}
        </div>
      ) : null}
    </div>
  );
}
