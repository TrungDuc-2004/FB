import { useEffect, useMemo, useRef, useState } from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";
import { DashboardIcons } from "./DashboardShell";
import "../styles/user/layout.css";

const AVATAR_STORAGE_KEY = "account_avatar";
const RECENT_SEARCHES_KEY = "user_recent_searches_v1";
const MAX_RECENT = 5;

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
  const next = [text, ...current.filter((item) => item !== text)].slice(0, MAX_RECENT);
  localStorage.setItem(RECENT_SEARCHES_KEY, JSON.stringify(next));
}

function removeRecentSearch(keyword) {
  const next = readRecentSearches().filter((item) => item !== keyword);
  localStorage.setItem(RECENT_SEARCHES_KEY, JSON.stringify(next));
  return next;
}

function clearRecentSearches() {
  localStorage.removeItem(RECENT_SEARCHES_KEY);
}

function SearchIcon({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <circle cx="9" cy="9" r="5" stroke="currentColor" strokeWidth="1.6" />
      <path d="M13 13l4 4" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
    </svg>
  );
}

function CloseIcon({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <path d="M5 5l10 10" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      <path d="M15 5L5 15" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  );
}

function UserAvatar({ avatar, letter, large = false }) {
  return (
    <div className={`user-site-avatar${large ? " large" : ""}`}>
      {avatar ? <img src={avatar} alt="Ảnh đại diện" /> : <span>{letter}</span>}
    </div>
  );
}

function HeaderSearch({ value, onChange, onSubmit, recentSearches, showRecent, onFocus, recentBoxRef, onClickRecent, onRemoveRecent, onClearRecentAll }) {
  return (
    <div className="user-site-search-shell" ref={recentBoxRef}>
      <form className="user-site-search" onSubmit={onSubmit}>
        <span className="user-site-search-icon" aria-hidden="true">
          <SearchIcon />
        </span>
        <input
          value={value}
          onChange={(event) => onChange(event.target.value)}
          onFocus={onFocus}
          placeholder="Tìm tài liệu, chủ đề, bài học..."
          aria-label="Tìm kiếm tài liệu"
        />
        {value ? (
          <button
            className="user-site-search-clear"
            type="button"
            onClick={() => onChange("")}
            aria-label="Xóa từ khóa"
          >
            <CloseIcon size={18} />
          </button>
        ) : null}
        <button className="user-site-search-submit" type="submit" aria-label="Tìm kiếm">
          <SearchIcon size={18} />
        </button>
      </form>

      {showRecent && recentSearches.length > 0 ? (
        <div className="user-site-search-popover">
          <div className="user-site-search-popover-head">
            <strong>5 tìm kiếm gần nhất</strong>
            <button type="button" onClick={onClearRecentAll}>
              Xóa tất cả
            </button>
          </div>

          <div className="user-site-search-recent-list">
            {recentSearches.map((item) => (
              <div key={item} className="user-site-search-recent-item">
                <button type="button" className="main" onClick={() => onClickRecent(item)}>
                  <span>🕘</span>
                  <span>{item}</span>
                </button>
                <button type="button" className="remove" onClick={() => onRemoveRecent(item)} aria-label={`Xóa ${item}`}>
                  ×
                </button>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function QuickInfoCard({ title, value }) {
  return (
    <div className="user-site-quick-card">
      <span>{title}</span>
      <strong>{value}</strong>
    </div>
  );
}

export default function UserLayout() {
  const { IHome, IBook, ISearch, IStar, IUser } = DashboardIcons;
  const navigate = useNavigate();
  const location = useLocation();
  const menuRef = useRef(null);
  const recentBoxRef = useRef(null);

  const [menuOpen, setMenuOpen] = useState(false);
  const [globalQuery, setGlobalQuery] = useState("");
  const [showRecent, setShowRecent] = useState(false);
  const [recentSearches, setRecentSearches] = useState(() => readRecentSearches());

  const username = (localStorage.getItem("username") || "Người dùng").trim() || "Người dùng";
  const role = (localStorage.getItem("role") || "user").toLowerCase();
  const avatar = localStorage.getItem(AVATAR_STORAGE_KEY) || "";
  const avatarLetter = (username[0] || "U").toUpperCase();
  const roleLabel = role === "admin" ? "Quản trị viên" : "Sinh viên";

  const navItems = useMemo(
    () => [
      { to: "/user", label: "Khám phá", icon: IHome, end: true },
      { to: "/user/search", label: "Tìm kiếm", icon: ISearch },
      { to: "/user/library", label: "Thư viện", icon: IBook },
      { to: "/user/saved", label: "Đã lưu", icon: IStar },
      { to: "/user/profile", label: "Hồ sơ", icon: IUser },
    ],
    [IBook, IHome, ISearch, IStar, IUser]
  );

  useEffect(() => {
    document.body.classList.add("user-shell-clean");
    return () => {
      document.body.classList.remove("user-shell-clean");
    };
  }, []);

    useEffect(() => {
    const params = new URLSearchParams(location.search || "");
    const nextQuery = params.get("q") || "";
    const frame = window.requestAnimationFrame(() => setGlobalQuery(nextQuery));
    return () => window.cancelAnimationFrame(frame);
  }, [location.pathname, location.search]);

  useEffect(() => {
    function handleClickOutside(event) {
      if (menuRef.current && !menuRef.current.contains(event.target)) {
        setMenuOpen(false);
      }
      if (recentBoxRef.current && !recentBoxRef.current.contains(event.target)) {
        setShowRecent(false);
      }
    }

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  function goToSearch(keyword) {
    const text = String(keyword || "").trim();
    const params = new URLSearchParams();
    if (text) params.set("q", text);
    navigate(`/user/search${params.toString() ? `?${params.toString()}` : ""}`);
    setShowRecent(false);
    setMenuOpen(false);
  }

  function handleSearchSubmit(event) {
    event.preventDefault();
    const keyword = globalQuery.trim();
    if (keyword) {
      saveRecentSearch(keyword);
      setRecentSearches(readRecentSearches());
    }
    goToSearch(keyword);
  }

  function handleClickRecent(keyword) {
    setGlobalQuery(keyword);
    saveRecentSearch(keyword);
    setRecentSearches(readRecentSearches());
    goToSearch(keyword);
  }

  function handleRemoveRecent(keyword) {
    setRecentSearches(removeRecentSearch(keyword));
  }

  function handleClearRecentAll() {
    clearRecentSearches();
    setRecentSearches([]);
  }

  function logout() {
    localStorage.removeItem("role");
    localStorage.removeItem("user_id");
    localStorage.removeItem("username");
    localStorage.removeItem(AVATAR_STORAGE_KEY);
    navigate("/login");
  }

  return (
    <div className="user-site-shell">
      <header className="user-site-header">
        <div className="user-site-header-main">
          <button className="user-site-brand" type="button" onClick={() => navigate("/user")}>
            <div className="user-site-brand-mark">D</div>
            <div className="user-site-brand-copy">
              <strong>DocSpace</strong>
              <span>Kho học liệu số cho người dùng</span>
            </div>
          </button>

          <HeaderSearch
            value={globalQuery}
            onChange={setGlobalQuery}
            onSubmit={handleSearchSubmit}
            recentSearches={recentSearches}
            showRecent={showRecent}
            onFocus={() => setShowRecent(true)}
            recentBoxRef={recentBoxRef}
            onClickRecent={handleClickRecent}
            onRemoveRecent={handleRemoveRecent}
            onClearRecentAll={handleClearRecentAll}
          />

          <div className="user-site-header-right">
            <div className="user-site-quick-wrap">
              <QuickInfoCard title="Tra cứu" value="Nhanh hơn" />
              <QuickInfoCard title="Giao diện" value="Gọn & hiện đại" />
            </div>

            <div className="user-site-user-menu" ref={menuRef}>
              <button
                className={`user-site-user-trigger${menuOpen ? " open" : ""}`}
                type="button"
                onClick={() => setMenuOpen((prev) => !prev)}
                aria-expanded={menuOpen}
                aria-label="Mở menu tài khoản"
              >
                <UserAvatar avatar={avatar} letter={avatarLetter} />
                <div className="user-site-user-copy">
                  <strong>{username}</strong>
                  <span>{roleLabel}</span>
                </div>
                <svg width="18" height="18" viewBox="0 0 20 20" fill="none" aria-hidden="true">
                  <path d="m5 8 5 5 5-5" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              </button>

              {menuOpen ? (
                <div className="user-site-user-dropdown">
                  <div className="user-site-user-dropdown-head">
                    <UserAvatar avatar={avatar} letter={avatarLetter} large />
                    <div>
                      <strong>{username}</strong>
                      <span>{roleLabel}</span>
                    </div>
                  </div>

                  <div className="user-site-user-dropdown-actions">
                    <button type="button" onClick={() => { navigate("/user/profile"); setMenuOpen(false); }}>
                      Mở hồ sơ
                    </button>
                    <button type="button" onClick={() => { navigate("/user/saved"); setMenuOpen(false); }}>
                      Xem tài liệu đã lưu
                    </button>
                    <button type="button" onClick={logout}>
                      Đăng xuất
                    </button>
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        </div>

        <div className="user-site-nav-row">
          <nav className="user-site-nav" aria-label="Điều hướng người dùng">
            {navItems.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                end={Boolean(item.end)}
                className={({ isActive }) => `user-site-nav-item${isActive ? " active" : ""}`}
              >
                <span className="user-site-nav-icon">{item.icon}</span>
                <span>{item.label}</span>
              </NavLink>
            ))}
          </nav>

          <div className="user-site-nav-note">
            <span>Sắp xếp chức năng lên thanh trên để trang tìm kiếm thoáng hơn.</span>
          </div>
        </div>
      </header>

      <main className="user-site-content">
        <Outlet />
      </main>

      <nav className="user-site-mobile-dock" aria-label="Điều hướng nhanh trên di động">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={Boolean(item.end)}
            className={({ isActive }) => `user-site-mobile-item${isActive ? " active" : ""}`}
          >
            <span className="user-site-mobile-icon">{item.icon}</span>
            <span>{item.label}</span>
          </NavLink>
        ))}
      </nav>
    </div>
  );
}
