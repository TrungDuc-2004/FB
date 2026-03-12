import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import "../../styles/admin/page.css";
import { HOME_PRESETS } from "../../data/userPresets";

function buildSearchUrl({ q = "", classID = "", subjectID = "", topicID = "", lessonID = "" } = {}) {
  const sp = new URLSearchParams();
  if (q) sp.set("q", q);
  if (classID) sp.set("classID", classID);
  if (subjectID) sp.set("subjectID", subjectID);
  if (topicID) sp.set("topicID", topicID);
  if (lessonID) sp.set("lessonID", lessonID);
  return `/user/search${sp.toString() ? `?${sp.toString()}` : ""}`;
}

export default function UserHome() {
  const navigate = useNavigate();
  const [q, setQ] = useState("");

  const quickStats = useMemo(
    () => [
      { label: "Tra cứu nhanh", value: "Tìm theo câu hỏi hoặc từ khóa" },
      { label: "Duyệt theo danh mục", value: "Lớp → Môn → Chủ đề → Bài" },
      { label: "Lưu lại", value: "Giữ tài liệu quan trọng để xem lại" },
    ],
    []
  );

  function goSearch(e) {
    e?.preventDefault?.();
    const s = q.trim();
    if (!s) return;
    navigate(buildSearchUrl({ q: s }));
  }

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <div
        className="page-header"
        style={{
          background: "linear-gradient(135deg, #0f172a 0%, #1d4ed8 55%, #38bdf8 100%)",
          color: "#fff",
          overflow: "hidden",
          position: "relative",
        }}
      >
        <div style={{ position: "absolute", inset: 0, opacity: 0.14, background: "radial-gradient(circle at top right, #ffffff 0, transparent 32%)" }} />
        <div style={{ position: "relative", display: "grid", gridTemplateColumns: "minmax(0, 1.7fr) minmax(280px, 1fr)", gap: 20 }}>
          <div>
            <div style={{ fontSize: 13, fontWeight: 700, opacity: 0.9, letterSpacing: 0.3, marginBottom: 10 }}>KHO TÀI LIỆU NGƯỜI DÙNG</div>
            <div style={{ fontSize: 30, fontWeight: 800, lineHeight: 1.2, marginBottom: 10 }}>Tra cứu, duyệt danh mục và lưu tài liệu ở cùng một nơi.</div>
            <div style={{ maxWidth: 720, lineHeight: 1.7, color: "rgba(255,255,255,0.9)", marginBottom: 16 }}>
              Bạn có thể tìm theo câu hỏi tự nhiên, mở nhanh các danh sách mẫu hoặc đi vào thư viện để chọn theo lớp, môn, chủ đề và bài học.
            </div>

            <form onSubmit={goSearch} style={{ display: "flex", gap: 10, flexWrap: "wrap", marginBottom: 14 }}>
              <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="Ví dụ: cấu trúc dữ liệu hàng đợi, nghị luận xã hội, ôn tập hình học..."
                style={{
                  flex: "1 1 360px",
                  height: 46,
                  borderRadius: 14,
                  border: "1px solid rgba(255,255,255,0.24)",
                  background: "rgba(255,255,255,0.16)",
                  color: "#fff",
                  padding: "0 14px",
                  outline: "none",
                }}
              />
              <button className="btn" type="submit" style={{ height: 46, background: "#fff", color: "#0f172a", borderColor: "#fff" }}>
                Tìm kiếm ngay
              </button>
              <button className="btn" type="button" style={{ height: 46, background: "transparent", color: "#fff", borderColor: "rgba(255,255,255,0.32)" }} onClick={() => navigate("/user/library")}>
                Mở thư viện
              </button>
            </form>

            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              {["Tìm theo câu hỏi", "Có danh sách mẫu", "Lưu tài liệu 1 chạm"].map((item) => (
                <span key={item} style={{ padding: "7px 10px", borderRadius: 999, background: "rgba(255,255,255,0.14)", border: "1px solid rgba(255,255,255,0.16)", fontSize: 12.5, fontWeight: 700 }}>
                  {item}
                </span>
              ))}
            </div>
          </div>

          <div style={{ display: "grid", gap: 10 }}>
            {quickStats.map((item) => (
              <div key={item.label} style={{ background: "rgba(255,255,255,0.12)", border: "1px solid rgba(255,255,255,0.16)", borderRadius: 16, padding: 14 }}>
                <div style={{ fontSize: 12.5, opacity: 0.86, marginBottom: 6 }}>{item.label}</div>
                <div style={{ fontSize: 16, fontWeight: 800 }}>{item.value}</div>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row" style={{ justifyContent: "space-between", alignItems: "center" }}>
            <div>
              <div className="page-title">Danh sách gợi ý có sẵn</div>
              <div className="breadcrumb">
                <span className="crumb">User</span>
                <span className="crumb">Trang chủ</span>
                <span className="crumb">Gợi ý nhanh</span>
              </div>
            </div>
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(250px, 1fr))", gap: 14 }}>
          {HOME_PRESETS.map((preset) => (
            <div key={preset.id} style={{ border: "1px solid #dbeafe", background: "linear-gradient(180deg, #f8fbff 0%, #ffffff 100%)", borderRadius: 18, padding: 16, display: "grid", gap: 12 }}>
              <div>
                <div style={{ fontSize: 18, fontWeight: 800, color: "#0f172a", marginBottom: 8 }}>{preset.title}</div>
                <div style={{ color: "#475569", lineHeight: 1.6 }}>{preset.description}</div>
              </div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                {preset.chips.map((chip) => (
                  <span key={chip} className="crumb">
                    {chip}
                  </span>
                ))}
              </div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                <button className="btn btn-primary" type="button" onClick={() => navigate(`/user/library?preset=${encodeURIComponent(preset.libraryPresetId)}`)}>
                  Mở danh sách này
                </button>
                <button className="btn" type="button" onClick={() => navigate(buildSearchUrl({ q: preset.searchQuery }))}>
                  Tìm tương tự
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>

      <div className="table-wrapper" style={{ padding: 18 }}>
        <div style={{ color: "#475569", lineHeight: 1.75 }}>
          <div style={{ fontWeight: 800, color: "#0f172a", marginBottom: 10 }}>Luồng dùng gọn nhất</div>
          <div>1. Chọn một danh sách mẫu để vào nhanh.</div>
          <div>2. Nếu cần chính xác hơn, sang <b>Danh sách</b> để duyệt theo lớp → môn → chủ đề → bài.</div>
          <div>3. Khi gặp tài liệu quan trọng, dùng nút <b>Lưu</b> để xem lại trong mục <b>Đã lưu</b>.</div>
        </div>
      </div>
    </div>
  );
}
