import { useState } from "react";
import { useNavigate } from "react-router-dom";
import "../../styles/admin/page.css";

export default function UserHome() {
  const navigate = useNavigate();
  const [q, setQ] = useState("");

  function goSearch(e) {
    e?.preventDefault?.();
    const s = q.trim();
    if (!s) return;
    navigate(`/user/search?q=${encodeURIComponent(s)}`);
  }

  return (
    <div>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <div>
              <div className="page-title">Tra cứu tài liệu</div>
              <div className="breadcrumb">
                <span className="crumb">User</span>
                <span className="crumb">Trang chủ</span>
              </div>
            </div>
          </div>
        </div>

        <div className="page-header-bottom">
          <form className="search-box" onSubmit={goSearch}>
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Nhập câu truy vấn để tìm tài liệu..."
            />
          </form>
          <div className="header-actions">
            <button className="btn btn-primary" onClick={goSearch} type="button">
              Tìm kiếm
            </button>
            <button className="btn" type="button" onClick={() => navigate("/user/list")}
            >
              Danh sách
            </button>
            <button className="btn" type="button" onClick={() => navigate("/user/saved")}
            >
              Đã lưu
            </button>
          </div>
        </div>
      </div>

      <div className="table-wrapper" style={{ padding: 18 }}>
        <div style={{ color: "#475569", lineHeight: 1.7 }}>
          <div style={{ fontWeight: 700, color: "#0f172a", marginBottom: 8 }}>Gợi ý</div>
          <ul style={{ paddingLeft: 18 }}>
            <li>Tìm theo câu hỏi/đề bài (hệ thống sẽ trích xuất keyword và so sánh embedding).</li>
            <li>Vào <b>Danh sách</b> để duyệt theo lớp → môn → chủ đề → bài → tài liệu.</li>
            <li>Trong <b>Chi tiết tài liệu</b> có nút <b>Xem</b> và <b>Lưu</b>.</li>
          </ul>
        </div>
      </div>
    </div>
  );
}

