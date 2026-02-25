import { useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";

import "../../styles/admin/page.css";
import DataTable from "../../components/DataTable";
import * as api from "../../api/userDocsApi";

function extFromUrl(u = "") {
  const s = String(u || "");
  const noQ = s.split("?")[0];
  const i = noQ.lastIndexOf(".");
  return i >= 0 ? noQ.slice(i + 1).toLowerCase() : "";
}

export default function UserDocDetail() {
  const navigate = useNavigate();
  // Route đang dùng :chunkID nên lấy đúng tên param
  const { chunkID } = useParams();

  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [data, setData] = useState(null);
  const [saving, setSaving] = useState(false);

  async function load() {
    if (!chunkID) return;
    setLoading(true);
    setErr("");
    try {
      const d = await api.getChunkDetail(chunkID);
      setData(d);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, [chunkID]);

  const chunk = data?.chunk;
  const keywords = data?.keywords || [];
  const related = data?.related || [];

  const relatedRows = useMemo(
    () =>
      related.map((x, idx) => ({
        id: x.chunkID || String(idx),
        ...x,
        fileExt: extFromUrl(x.chunkUrl),
      })),
    [related]
  );

  const relatedCols = [
    { key: "chunkID", label: "Mã", width: "220px" },
    { key: "chunkName", label: "Tên" },
    { key: "fileExt", label: "File", width: "90px" },
  ];

  async function toggleSave() {
    if (!chunkID) return;
    setSaving(true);
    try {
      await api.toggleSave(chunkID);
    } catch (e) {
      alert(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div>
              <div className="page-title">Chi tiết tài liệu</div>
              <div className="breadcrumb">
                <span className="crumb">User</span>
                <span className="crumb">Tài liệu</span>
                <span className="crumb">Chi tiết</span>
              </div>
            </div>
            <button className="back-btn back-btn-right" onClick={() => navigate(-1)} type="button">
              ← Quay lại
            </button>
          </div>
        </div>

        <div className="page-header-bottom">
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", width: "100%" }}>
            <div style={{ fontWeight: 800, color: "#0f172a" }}>{chunkID}</div>
            <button className="btn" type="button" onClick={() => navigate(`/user/view/${encodeURIComponent(chunkID)}`)}>
              Xem
            </button>
            <button className="btn" type="button" onClick={toggleSave} disabled={saving}>
              ★ Lưu
            </button>
          </div>
        </div>
      </div>

      <div className="table-wrapper" style={{ padding: 18, marginBottom: 16 }}>
        {err ? <div style={{ color: "#b91c1c" }}>Lỗi: {err}</div> : null}
        {loading && !chunk ? <div className="empty-state">Đang tải...</div> : null}

        {chunk ? (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
            <div>
              <div style={{ fontWeight: 800, marginBottom: 6, color: "#0f172a" }}>Thông tin</div>
              <div style={{ color: "#334155", lineHeight: 1.7 }}>
                <div>
                  <b>Tên:</b> {chunk.chunkName || "(không có)"}
                </div>
                <div>
                  <b>Loại:</b> {chunk.chunkType || "-"}
                </div>
                <div>
                  <b>Bài:</b> {chunk.lessonID || "-"}
                </div>
                <div>
                  <b>URL:</b> {chunk.chunkUrl ? (
                    <a href={chunk.chunkUrl} target="_blank" rel="noreferrer">
                      Mở file
                    </a>
                  ) : (
                    "-"
                  )}
                </div>
                <div>
                  <b>Mô tả:</b> {chunk.chunkDescription || "-"}
                </div>
              </div>
            </div>

            <div>
              <div style={{ fontWeight: 800, marginBottom: 6, color: "#0f172a" }}>Keyword</div>
              {keywords.length === 0 ? (
                <div style={{ color: "#64748b" }}>Chưa có keyword.</div>
              ) : (
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  {keywords.map((k) => (
                    <span
                      key={k.keywordID}
                      className="crumb"
                      title={k.keywordID}
                      style={{ cursor: "default" }}
                    >
                      {k.keywordName}
                    </span>
                  ))}
                </div>
              )}
            </div>
          </div>
        ) : null}
      </div>

      <div className="table-wrapper">
        <div style={{ padding: "14px 16px", borderBottom: "1px solid #f1f5f9", fontWeight: 800 }}>
          Tài liệu liên quan (cùng bài)
        </div>

        {relatedRows.length === 0 ? (
          <div className="empty-state">Không có tài liệu liên quan.</div>
        ) : (
          <DataTable
            columns={relatedCols}
            rows={relatedRows}
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
