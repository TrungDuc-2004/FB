import { useLayoutEffect, useMemo, useRef, useState } from "react";
import "../styles/admin/table.css";

export default function DataTable({
  columns,
  rows = [],
  renderActions,
  onRowDoubleClick,
  getRowClassName,
  pageSize, // nếu không truyền => auto
  showPagination = true,
}) {
  const total = rows.length;

  const scrollRef = useRef(null);
  const [autoSize, setAutoSize] = useState(10); // default tạm

  // ✅ đo chiều cao để tính số row fit màn hình
  useLayoutEffect(() => {
    if (pageSize === -1 || (pageSize && pageSize > 0)) return; // có pageSize hoặc hiển thị toàn bộ thì khỏi auto

    const el = scrollRef.current;
    if (!el) return;

    const compute = () => {
      const tbodyRow = el.querySelector("tbody tr");
      const rowH = tbodyRow ? tbodyRow.getBoundingClientRect().height : 64; // fallback
      const head = el.querySelector("thead");
      const headH = head ? head.getBoundingClientRect().height : 0;

      const available = el.getBoundingClientRect().height - headH;
      const n = Math.max(7, Math.floor(available / rowH)); // tối thiểu 3 dòng
      setAutoSize(n);
    };

    compute();

    const ro = new ResizeObserver(() => compute());
    ro.observe(el);

    // nếu font load xong làm đổi chiều cao row -> tính lại
    window.addEventListener("resize", compute);

    return () => {
      ro.disconnect();
      window.removeEventListener("resize", compute);
    };
  }, [pageSize, columns.length, rows.length]);

  // ✅ size cuối cùng
  const size = pageSize === -1 ? 0 : pageSize && pageSize > 0 ? pageSize : autoSize;
  const totalPages = size > 0 ? Math.max(1, Math.ceil(total / size)) : 1;
  const datasetKey = `${total}:${size}`;

  const [pagination, setPagination] = useState(() => ({ key: datasetKey, page: 1 }));
  const page = pagination.key === datasetKey ? Math.min(pagination.page, totalPages) : 1;

  function updatePage(nextPageOrUpdater) {
    setPagination((prev) => {
      const basePage = prev.key === datasetKey ? Math.min(prev.page, totalPages) : 1;
      const rawNext =
        typeof nextPageOrUpdater === "function" ? nextPageOrUpdater(basePage) : nextPageOrUpdater;
      const safeNext = Math.min(totalPages, Math.max(1, Number(rawNext) || 1));
      return { key: datasetKey, page: safeNext };
    });
  }

  const visibleRows = useMemo(() => {
    if (!size || size <= 0) return rows;
    const start = (page - 1) * size;
    return rows.slice(start, start + size);
  }, [rows, page, size]);

  const startIdx = total === 0 ? 0 : (page - 1) * size + 1;
  const endIdx = total === 0 ? 0 : Math.min(page * size, total);

  return (
    <div className="table-container">
      <div className="table-scroll" ref={scrollRef}>
        <table className="table">
          <thead>
            <tr>
              {columns.map((c) => (
                <th key={c.key} style={{ width: c.width || "auto" }}>
                  {c.label}
                </th>
              ))}
              {renderActions ? (
                <th style={{ textAlign: "right", width: "180px" }}>THAO TÁC</th>
              ) : null}
            </tr>
          </thead>

          <tbody>
            {visibleRows.map((row) => (
              <tr
                key={row.id}
                className={getRowClassName ? getRowClassName(row) : undefined}
                onDoubleClick={onRowDoubleClick ? () => onRowDoubleClick(row) : undefined}
              >
                {columns.map((c) => (
                  <td key={c.key}>{c.render ? c.render(row) : row[c.key]}</td>
                ))}
                {renderActions ? (
                  <td style={{ textAlign: "right" }}>{renderActions(row)}</td>
                ) : null}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {showPagination && size > 0 && total > size ? (
        <div className="table-pagination">
          <div className="table-pagination__info">
            Hiển thị {startIdx}-{endIdx} / {total}
          </div>

          <div className="table-pagination__controls">
            <button
              type="button"
              className="btn"
              disabled={page <= 1}
              onClick={() => updatePage((p) => Math.max(1, p - 1))}
            >
              ← Trước
            </button>

            <div className="table-pagination__page">
              Trang {page}/{totalPages}
            </div>

            <button
              type="button"
              className="btn"
              disabled={page >= totalPages}
              onClick={() => updatePage((p) => Math.min(totalPages, p + 1))}
            >
              Sau →
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
