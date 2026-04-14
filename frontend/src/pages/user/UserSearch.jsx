import { useEffect, useMemo, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import "../../styles/user/search.css";
import DocumentCard from "../../components/DocumentCard";
import { searchDocs, toggleSave } from "../../services/userDocsApi";

const DATE_OPTIONS = [
  { value: "today", label: "Hôm nay" },
  { value: "7d", label: "7 ngày gần đây" },
  { value: "30d", label: "30 ngày gần đây" },
  { value: "90d", label: "90 ngày gần đây" },
];

function FilterSelect({ value, onChange, options, allLabel = "Tất cả", disabled }) {
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
  const normalized = (Array.isArray(items) ? items : []).map((item) => ({
    ...item,
    itemType: item?.itemType || item?.type || "chunk",
    type: item?.type || item?.itemType || "chunk",
    chunkID: item?.chunkID || item?.id || "",
    chunkName: item?.chunkName || item?.name || item?.id || "Chưa có tên",
  }));

  return uniqueBy(
    normalized,
    (item) =>
      `${item?.itemType || item?.type || item?.category || "document"}::${item?.chunkID || item?.id}`
  );
}

function typeRank(item) {
  const kind = item?.itemType || item?.type || "chunk";
  return { chunk: 0, lesson: 1, topic: 2, subject: 3, image: 4, video: 5 }[kind] ?? 99;
}

function buildParams({ q = "", classID = "", date = "" } = {}) {
  const sp = new URLSearchParams();
  if (String(q || "").trim()) sp.set("q", String(q).trim());
  if (classID) sp.set("classID", classID);
  if (date) sp.set("date", date);
  return sp;
}

function prettyCount(value) {
  return new Intl.NumberFormat("vi-VN").format(Number(value || 0));
}

function parseItemDate(item) {
  const raw =
    item?.updatedAt ||
    item?.createdAt ||
    item?.updated_at ||
    item?.created_at ||
    item?.date ||
    "";

  if (!raw) return null;

  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return null;
  return date;
}

function isSameLocalDay(a, b) {
  return (
    a.getFullYear() === b.getFullYear() &&
    a.getMonth() === b.getMonth() &&
    a.getDate() === b.getDate()
  );
}

function isDateMatched(item, dateFilter) {
  if (!dateFilter) return true;

  const itemDate = parseItemDate(item);
  if (!itemDate) return false;

  const now = new Date();

  if (dateFilter === "today") {
    return isSameLocalDay(itemDate, now);
  }

  const mapDays = {
    "7d": 7,
    "30d": 30,
    "90d": 90,
  };

  const days = mapDays[dateFilter];
  if (!days) return true;

  const threshold = new Date(now);
  threshold.setHours(0, 0, 0, 0);
  threshold.setDate(threshold.getDate() - days);

  return itemDate >= threshold;
}

function normalizeClassOption(item) {
  const classID = String(item?.class?.classID || "").trim();
  const className = String(item?.class?.className || "").trim();

  if (!classID && !className) return null;

  return {
    value: classID || className,
    label: className || classID,
  };
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

  const [classID, setClassID] = useState("");
  const [dateFilter, setDateFilter] = useState("");
  const [rawItems, setRawItems] = useState([]);

  const reqSeqRef = useRef(0);

  const classOptions = useMemo(
    () =>
      uniqueBy(
        rawItems
          .map(normalizeClassOption)
          .filter(Boolean)
          .sort((a, b) => a.label.localeCompare(b.label, "vi")),
        (item) => item.value
      ),
    [rawItems]
  );

  const displayedItems = useMemo(() => {
    return rawItems.filter((item) => {
      const itemClassID = String(item?.class?.classID || "").trim();

      if (classID && itemClassID !== classID) return false;
      if (!isDateMatched(item, dateFilter)) return false;

      return true;
    });
  }, [rawItems, classID, dateFilter]);

  useEffect(() => {
    const sp = new URLSearchParams(location.search || "");
    setQ((sp.get("q") || "").trim());
    setClassID((sp.get("classID") || "").trim());
    setDateFilter((sp.get("date") || "").trim());
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

        const response = await searchDocs({
          q,
          classID,
          category: "all",
          limit: 100,
          offset: 0,
        });

        const normalized = normalizeSearchResponse(response);

        const items = aggregateSearchItems(
          (normalized.items || []).map((item) => ({
            ...item,
            category: item?.category || "all",
          }))
        ).sort((a, b) => {
          const rankDiff = typeRank(a) - typeRank(b);
          if (rankDiff !== 0) return rankDiff;

          const scoreDiff = Number(b?.score || 0) - Number(a?.score || 0);
          if (scoreDiff !== 0) return scoreDiff;

          return String(a?.chunkName || "").localeCompare(String(b?.chunkName || ""), "vi");
        });

        if (seq !== reqSeqRef.current) return;

        setRawItems(
          uniqueBy(
            items,
            (item) =>
              `${item.itemType || item.type || item.category || "document"}::${item.chunkID || item.id}`
          )
        );
      } catch (error) {
        if (seq !== reqSeqRef.current) return;
        setErr(String(error?.message || error));
        setRawItems([]);
      } finally {
        if (seq === reqSeqRef.current) setLoading(false);
      }
    })();
  }, [q, classID]);

  useEffect(() => {
    if (!classID) return;

    const exists = classOptions.some((option) => option.value === classID);
    if (!exists && rawItems.length > 0) {
      const sp = buildParams({ q, classID: "", date: dateFilter });
      navigate(`/user/search${sp.toString() ? `?${sp.toString()}` : ""}`, { replace: true });
    }
  }, [classID, classOptions, dateFilter, navigate, q, rawItems.length]);

  function updateSearch(next = {}) {
    const sp = buildParams({
      q,
      classID: typeof next.classID === "string" ? next.classID : classID,
      date: typeof next.date === "string" ? next.date : dateFilter,
    });

    navigate(`/user/search${sp.toString() ? `?${sp.toString()}` : ""}`, { replace: true });
  }

  function handleClassChange(value) {
    updateSearch({ classID: value });
  }

  function handleDateChange(value) {
    updateSearch({ date: value });
  }

  function onClearFilters() {
    updateSearch({ classID: "", date: "" });
  }

  async function onToggleSave(doc) {
    try {
      if (!doc?.chunkID) return;

      const currentCategory = doc?.category || "document";
      const response = await toggleSave(
        doc.chunkID,
        currentCategory === "all" ? "document" : currentCategory
      );

      setRawItems((prev) =>
        (prev || []).map((item) =>
          item.chunkID === doc.chunkID &&
          (item.category || "document") === (doc.category || "document")
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
          value={dateFilter}
          onChange={handleDateChange}
          options={DATE_OPTIONS}
          allLabel="Theo date"
          disabled={loading || !q}
        />

        <FilterSelect
          value={classID}
          onChange={handleClassChange}
          options={classOptions}
          allLabel="Theo lớp"
          disabled={loading || !q || !classOptions.length}
        />

        <button
          className="search-filter-clear"
          type="button"
          onClick={onClearFilters}
          disabled={loading || !q}
        >
          Clear all
        </button>
      </div>

      <div className="search-results-header">
        <div>
          <div className="search-results-title">
            {q ? `Kết quả cho “${q}”` : "Kết quả tìm kiếm"}
          </div>
          <div className="search-results-subtitle">
            {shown > 0
              ? `${prettyCount(shown)} mục phù hợp`
              : "Dùng ô tìm kiếm ở thanh trên để bắt đầu."}
          </div>
        </div>
      </div>

      {err ? <div className="search-inline-message danger">Lỗi: {err}</div> : null}
      {loading ? <SearchSkeleton count={4} /> : null}

      {!loading && !err && !hasSearchContext ? (
        <div className="search-empty-state">
          Nhập từ khóa ở ô tìm kiếm phía trên để bắt đầu tìm kiếm.
        </div>
      ) : null}

      {!loading && !err && hasSearchContext && shown === 0 ? (
        <div className="search-empty-state">
          Không có kết quả phù hợp. Hãy thử đổi từ khóa hoặc nới bộ lọc.
        </div>
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