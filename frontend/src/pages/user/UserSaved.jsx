import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import "../../styles/admin/page.css";
import DataTable from "../../components/DataTable";
import * as api from "../../api/userDocsApi";

function extFromUrl(u = "") {
  const s = String(u || "");
  const noQ = s.split("?")[0];
  const i = noQ.lastIndexOf(".");
  return i >= 0 ? noQ.slice(i + 1).toLowerCase() : "";
}

export default function UserSaved() {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [items, setItems] = useState([]);

  async function load() {
    setLoading(true);
    setErr("");
    try {
      const data = await api.getSaved();
      setItems(data.items || []);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  const rows = useMemo(
    () =>
      (items || []).map((x, idx) => ({
        id: x.chunkID || String(idx),
        ...x,
        fileExt: extFromUrl(x.chunkUrl),
      })),
    [items]
  );

  const cols = [
    { key: "chunkID", label: "Mã", width: "220px" },
    { key: "chunkName", label: "Tên" },
    { key: "fileExt", label: "File", width: "90px" },
    { key: "lessonID", label: "Bài", width: "170px" },
  ];

  return (
    <div>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div>
              <div className="page-title">Tài liệu đã lưu</div>
              <div className="breadcrumb">
                <span className="crumb">User</span>
                <span className="crumb">Đã lưu</span>
              </div>
            </div>
          </div>
        </div>

        <div className="page-header-bottom">
          <div className="header-actions">
            <button className="btn" type="button" onClick={load} disabled={loading}>
              Tải lại
            </button>
            <button className="btn" type="button" onClick={() => navigate("/user/search")}
            >
              Tìm kiếm
            </button>
          </div>
        </div>
      </div>

      <div className="table-wrapper">
        {err ? <div style={{ padding: 14, color: "#b91c1c" }}>Lỗi: {err}</div> : null}
        {loading && rows.length === 0 ? (
          <div className="empty-state">Đang tải...</div>
        ) : rows.length === 0 ? (
          <div className="empty-state">Chưa có tài liệu nào được lưu.</div>
        ) : (
          <DataTable
            columns={cols}
            rows={rows}
            onRowDoubleClick={(r) => navigate(`/user/docs/${encodeURIComponent(r.chunkID)}`)}
            renderActions={(r) => (
              <div className="table-actions">
                <button className="btn" type="button" onClick={() => navigate(`/user/docs/${encodeURIComponent(r.chunkID)}`)}>
                  Chi tiết
                </button>
                <button className="btn btn-primary" type="button" onClick={() => navigate(`/user/view/${encodeURIComponent(r.chunkID)}`)}>
                  Xem
                </button>
              </div>
            )}
          />
        )}
      </div>
    </div>
  );
}
