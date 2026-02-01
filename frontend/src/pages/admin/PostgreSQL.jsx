// pages/admin/PostgreSQL.jsx
import { useEffect, useMemo, useState } from "react";
import "../../styles/admin/page.css";
import DataTable from "../../components/DataTable";
import * as pgApi from "../../services/postgreAdminApi";

function truncate(s = "", n = 48) {
  const str = String(s ?? "");
  return str.length > n ? str.slice(0, n) + "…" : str;
}

function rowTitle(row = {}) {
  return (
    row.class_name ||
    row.subject_name ||
    row.topic_name ||
    row.lesson_name ||
    row.chunk_name ||
    row.keyword_name ||
    row.username ||
    row.name ||
    ""
  );
}

export default function PostgreSQL() {
  // root -> table -> row detail
  const [currentTable, setCurrentTable] = useState("");
  const [currentPk, setCurrentPk] = useState("");
  const [q, setQ] = useState("");

  const [tables, setTables] = useState([]); // [{id,name}]
  const [rows, setRows] = useState([]); // raw rows from API
  const [totalRows, setTotalRows] = useState(0);

  const [err, setErr] = useState("");

  const isRoot = currentTable === "";
  const isRowDetail = !!currentPk;

  async function reloadTables() {
    setErr("");
    try {
      const data = await pgApi.listTables();
      const list = (data?.tables || []).map((name) => ({ id: name, name }));
      setTables(list);
    } catch (e) {
      setErr(String(e?.message || e));
      setTables([]);
    }
  }

  async function reloadRows(tableName) {
    if (!tableName) return;
    setErr("");
    try {
      const data = await pgApi.listRows(tableName, 500, 0);
      setRows(data?.rows || []);
      setTotalRows(data?.total ?? (data?.rows || []).length);
    } catch (e) {
      setErr(String(e?.message || e));
      setRows([]);
      setTotalRows(0);
    }
  }

  useEffect(() => {
    reloadTables();
  }, []);

  useEffect(() => {
    if (!currentTable) return;
    reloadRows(currentTable);
  }, [currentTable]);

  const headerTitle = useMemo(() => {
    if (isRoot) return "PostgreSQL";
    if (isRowDetail) {
      const r = rows.find((x) => String(x._pk) === String(currentPk)) || null;
      return rowTitle(r) || String(currentPk);
    }
    return currentTable;
  }, [isRoot, isRowDetail, currentTable, currentPk, rows]);

  const breadcrumbParts = useMemo(() => {
    if (isRoot) return [];
    if (isRowDetail) return ["postgres", currentTable, String(currentPk)];
    return ["postgres", currentTable];
  }, [isRoot, isRowDetail, currentTable, currentPk]);

  function goBack() {
    if (currentPk) {
      setCurrentPk("");
      setQ("");
      return;
    }
    setCurrentTable("");
    setQ("");
  }

  function openTable(row) {
    setCurrentTable(row.name);
    setCurrentPk("");
    setQ("");
  }

  // ===== Root: tables =====
  const tableRows = useMemo(() => {
    const s = q.trim().toLowerCase();
    const list = !s ? tables : tables.filter((t) => t.name.toLowerCase().includes(s));
    return list.slice().sort((a, b) => a.name.localeCompare(b.name));
  }, [tables, q]);

  // ===== Table: rows =====
  const dataRows = useMemo(() => {
    const s = q.trim().toLowerCase();

    const list = (rows || []).map((r) => {
      const title = rowTitle(r);
      const mongo = r?.mongo_id || "";
      const minio = r?.minio_url || "";

      return {
        ...r,
        id: String(r._pk), // DataTable needs id
        _title: title,
        _mongo_display: mongo,
        _minio_display: minio,
      };
    });

    const filtered = !s
      ? list
      : list.filter((r) => {
          const a = String(r._pk || "").toLowerCase();
          const b = String(r._title || "").toLowerCase();
          const c = String(r._mongo_display || "").toLowerCase();
          const d = String(r._minio_display || "").toLowerCase();
          return a.includes(s) || b.includes(s) || c.includes(s) || d.includes(s);
        });

    return filtered;
  }, [rows, q]);

  // ===== Detail: selected row =====
  const selectedRow = useMemo(() => {
    if (!currentTable || !currentPk) return null;
    return rows.find((r) => String(r._pk) === String(currentPk)) || null;
  }, [rows, currentTable, currentPk]);

  const fieldRows = useMemo(() => {
    if (!selectedRow) return [];
    const out = [];
    for (const [k, v] of Object.entries(selectedRow)) {
      out.push({
        id: k,
        k,
        v: typeof v === "string" ? v : JSON.stringify(v),
      });
    }
    return out;
  }, [selectedRow]);

  // ===== Columns =====
  const tableColumns = [
    {
      key: "name",
      label: "TABLE",
      render: (r) => (
        <div className="folder-cell">
          <div className="folder-left">
            <div className="folder-icon">🗃️</div>
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

  // View-only: hiển thị 4 cột chuẩn cho mọi table
  const dataColumns = [
    {
      key: "_pk",
      label: "PK",
      render: (r) => (
        <span className="crumb" title={String(r._pk || "")}>
          {truncate(String(r._pk || ""), 24)}
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
      key: "mongo_id",
      label: "MONGO ID",
      render: (r) => (
        <span className="crumb" title={String(r.mongo_id || "")}>
          {r.mongo_id ? String(r.mongo_id).slice(0, 10) + "…" : ""}
        </span>
      ),
    },
    {
      key: "minio_url",
      label: "MINIO URL",
      render: (r) => (
        <span title={r.minio_url || ""} style={{ whiteSpace: "nowrap" }}>
          {truncate(r.minio_url || "", 46)}
        </span>
      ),
    },
  ];

  const detailColumns = [
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
              placeholder={
                isRoot
                  ? "Tìm bảng..."
                  : isRowDetail
                    ? "Đang xem chi tiết (read-only)"
                    : `Tìm dữ liệu (${totalRows} rows) (pk/name/mongo/minio)...`
              }
              value={q}
              onChange={(e) => setQ(e.target.value)}
              disabled={isRowDetail}
            />
          </div>

          <span className="crumb" style={{ opacity: 0.7 }}>
            View only
          </span>

          <div className="header-actions" />
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
            columns={tableColumns}
            rows={tableRows}
            pageSize={10}
            getRowClassName={() => "row-click"}
            onRowDoubleClick={(row) => openTable(row)}
            renderActions={null}
          />
        ) : isRowDetail ? (
          <DataTable columns={detailColumns} rows={fieldRows} renderActions={null} />
        ) : (
          <DataTable
            columns={dataColumns}
            rows={dataRows}
            pageSize={10}
            getRowClassName={() => "row-click"}
            onRowDoubleClick={(row) => setCurrentPk(String(row._pk))}
            renderActions={null}
          />
        )}
      </div>
    </div>
  );
}
