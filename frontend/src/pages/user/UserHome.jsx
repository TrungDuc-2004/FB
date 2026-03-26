import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import DocumentCard from "../../components/DocumentCard";
import { getHomeFeed, toggleSave } from "../../services/userDocsApi";
import "../../styles/admin/page.css";
import "../../styles/user/home.css";

function buildSearchUrl({ q = "", classID = "", subjectID = "", topicID = "", lessonID = "" } = {}) {
  const sp = new URLSearchParams();
  if (q) sp.set("q", q);
  if (classID) sp.set("classID", classID);
  if (subjectID) sp.set("subjectID", subjectID);
  if (topicID) sp.set("topicID", topicID);
  if (lessonID) sp.set("lessonID", lessonID);
  return `/user/search${sp.toString() ? `?${sp.toString()}` : ""}`;
}

function SectionHeader({ title, subtitle, actionLabel, onAction }) {
  return (
    <div className="user-home-section-head">
      <div>
        <div className="user-home-section-title">{title}</div>
        {subtitle ? <div className="user-home-section-subtitle">{subtitle}</div> : null}
      </div>
      {actionLabel ? (
        <button className="btn" type="button" onClick={onAction}>
          {actionLabel}
        </button>
      ) : null}
    </div>
  );
}

function EmptyBlock({ text }) {
  return <div className="user-home-empty">{text}</div>;
}

export default function UserHome() {
  const navigate = useNavigate();
  const [q, setQ] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [feed, setFeed] = useState({
    stats: {},
    documents: [],
    images: [],
    videos: [],
  });

  const quickLinks = useMemo(
    () => [
      {
        label: "Tìm kiếm",
        hint: "Gõ câu hỏi tự nhiên hoặc từ khóa",
        onClick: () => navigate("/user/search"),
      },
      {
        label: "Đã lưu",
        hint: "Xem lại những nội dung quan trọng",
        onClick: () => navigate("/user/saved"),
      },
      {
        label: "Lịch sử",
        hint: "Mở lại những tài liệu đã xem gần đây",
        onClick: () => navigate("/user/history"),
      },
    ],
    [navigate]
  );

  async function loadFeed() {
    try {
      setLoading(true);
      setError("");
      const res = await getHomeFeed({ limit: 4 });
      setFeed({
        stats: res?.stats || {},
        documents: Array.isArray(res?.documents) ? res.documents : [],
        images: Array.isArray(res?.images) ? res.images : [],
        videos: Array.isArray(res?.videos) ? res.videos : [],
      });
    } catch (e) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadFeed();
  }, []);

  async function handleToggleSave(doc) {
    try {
      if (!doc?.chunkID) return;
      const category = doc?.category || doc?.itemType || "document";
      const res = await toggleSave(doc.chunkID, category);

      setFeed((prev) => ({
        ...prev,
        documents: (prev.documents || []).map((item) =>
          item.chunkID === doc.chunkID ? { ...item, isSaved: !!res.saved } : item
        ),
        images: (prev.images || []).map((item) =>
          item.chunkID === doc.chunkID ? { ...item, isSaved: !!res.saved } : item
        ),
        videos: (prev.videos || []).map((item) =>
          item.chunkID === doc.chunkID ? { ...item, isSaved: !!res.saved } : item
        ),
      }));
    } catch (e) {
      setError(String(e?.message || e));
    }
  }

  function goSearch(e) {
    e?.preventDefault?.();
    const value = q.trim();
    if (!value) return;
    navigate(buildSearchUrl({ q: value }));
  }

  const stats = [
    { label: "Tài liệu", value: feed?.stats?.documents ?? 0 },
    { label: "Hình ảnh", value: feed?.stats?.images ?? 0 },
    { label: "Video", value: feed?.stats?.videos ?? 0 },
  ];

  return (
    <div className="user-home-page">
      <section className="user-home-hero">
        <div className="user-home-hero-copy">
          <div className="user-home-eyebrow">TRANG CHỦ HỌC LIỆU</div>
          <h1>Tra cứu nhanh và mở ngay những nội dung đang có.</h1>
          <p>
            Toàn bộ tài liệu, hình ảnh và video mới nhất được gom ngay tại đây để bạn vào là thấy
            ngay, không cần đi vòng qua nhiều màn hình.
          </p>

          <form className="user-home-search" onSubmit={goSearch}>
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Ví dụ: phần mềm máy tính, mạng máy tính, dữ liệu ảnh số..."
            />
            <button className="btn btn-primary" type="submit">
              Tìm kiếm
            </button>
          </form>

          <div className="user-home-stat-row">
            {stats.map((item) => (
              <div key={item.label} className="user-home-stat-card">
                <span>{item.label}</span>
                <strong>{item.value}</strong>
              </div>
            ))}
          </div>
        </div>

        <div className="user-home-hero-side">
          {quickLinks.map((item) => (
            <button
              key={item.label}
              className="user-home-quick-link"
              type="button"
              onClick={item.onClick}
            >
              <strong>{item.label}</strong>
              <span>{item.hint}</span>
            </button>
          ))}
        </div>
      </section>

      {error ? <div className="user-home-inline-error">Lỗi: {error}</div> : null}

      <section className="user-home-section-shell">
        <SectionHeader
          title="Tài liệu đang có"
          subtitle="Các tài liệu mới cập nhật để mở nhanh từ trang chủ."
          actionLabel="Tìm tài liệu"
          onAction={() => navigate("/user/search")}
        />

        {loading && !feed.documents.length ? (
          <EmptyBlock text="Đang tải danh sách tài liệu..." />
        ) : null}

        {!loading && !feed.documents.length ? (
          <EmptyBlock text="Hiện chưa có tài liệu hiển thị trên trang chủ." />
        ) : null}

        <div className="user-home-card-list">
          {feed.documents.map((doc) => (
            <DocumentCard key={`doc-${doc.chunkID}`} doc={doc} onToggleSave={handleToggleSave} />
          ))}
        </div>
      </section>

      <section className="user-home-media-grid">
        <div className="user-home-section-shell compact">
          <SectionHeader
            title="Hình ảnh sẵn có"
            subtitle="Ảnh liên quan đã được đồng bộ và có thể mở trực tiếp."
            actionLabel="Tìm ảnh"
            onAction={() => navigate(buildSearchUrl({ q: "hình ảnh" }))}
          />

          {loading && !feed.images.length ? <EmptyBlock text="Đang tải ảnh..." /> : null}

          {!loading && !feed.images.length ? (
            <EmptyBlock text="Hiện chưa có hình ảnh hiển thị." />
          ) : null}

          <div className="user-home-card-list small">
            {feed.images.map((doc) => (
              <DocumentCard key={`img-${doc.chunkID}`} doc={doc} onToggleSave={handleToggleSave} />
            ))}
          </div>
        </div>

        <div className="user-home-section-shell compact">
          <SectionHeader
            title="Video sẵn có"
            subtitle="Video học liệu đang có trong hệ thống."
            actionLabel="Tìm video"
            onAction={() => navigate(buildSearchUrl({ q: "video" }))}
          />

          {loading && !feed.videos.length ? <EmptyBlock text="Đang tải video..." /> : null}

          {!loading && !feed.videos.length ? (
            <EmptyBlock text="Hiện chưa có video hiển thị." />
          ) : null}

          <div className="user-home-card-list small">
            {feed.videos.map((doc) => (
              <DocumentCard
                key={`video-${doc.chunkID}`}
                doc={doc}
                onToggleSave={handleToggleSave}
              />
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}