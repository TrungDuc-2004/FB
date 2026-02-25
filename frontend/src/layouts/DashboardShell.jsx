import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";
import "../styles/dashboard/layout.css";

function Icon({ children }) {
  return <span className="nav-icon">{children}</span>;
}

// ===== Sidebar icons (simple inline SVGs – no extra deps) =====
const IHome = (
  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
    <rect x="3" y="3" width="6" height="6" rx="1.5" fill="currentColor" opacity="0.9" />
    <rect x="11" y="3" width="6" height="6" rx="1.5" fill="currentColor" opacity="0.45" />
    <rect x="3" y="11" width="6" height="6" rx="1.5" fill="currentColor" opacity="0.45" />
    <rect x="11" y="11" width="6" height="6" rx="1.5" fill="currentColor" opacity="0.9" />
  </svg>
);

const IDatabase = (
  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
    <ellipse cx="10" cy="4.8" rx="6.5" ry="2.8" fill="currentColor" opacity="0.9" />
    <path
      d="M3.5 4.8V13.6c0 1.55 2.91 2.8 6.5 2.8s6.5-1.25 6.5-2.8V4.8"
      stroke="currentColor"
      strokeWidth="1.4"
      opacity="0.9"
    />
    <path
      d="M3.5 9.2c0 1.55 2.91 2.8 6.5 2.8s6.5-1.25 6.5-2.8"
      stroke="currentColor"
      strokeWidth="1.4"
      opacity="0.55"
    />
    <path
      d="M3.5 13.6c0 1.55 2.91 2.8 6.5 2.8s6.5-1.25 6.5-2.8"
      stroke="currentColor"
      strokeWidth="1.4"
      opacity="0.55"
    />
  </svg>
);

const IMinio = (
  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
    <path
      d="M5 8.2h10l-1.1 8.3H6.1L5 8.2Z"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinejoin="round"
    />
    <path
      d="M7.2 8.2V6.6c0-1.1.9-2 2-2h1.6c1.1 0 2 .9 2 2v1.6"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
    />
  </svg>
);

const INeo4j = (
  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
    <circle cx="5" cy="10" r="2" fill="currentColor" opacity="0.9" />
    <circle cx="15" cy="6" r="2" fill="currentColor" opacity="0.55" />
    <circle cx="15" cy="14" r="2" fill="currentColor" opacity="0.55" />
    <path d="M6.6 9.2 13.4 6.8" stroke="currentColor" strokeWidth="1.4" opacity="0.75" />
    <path d="M6.6 10.8 13.4 13.2" stroke="currentColor" strokeWidth="1.4" opacity="0.75" />
  </svg>
);

const IUser = (
  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
    <circle cx="10" cy="7" r="3" stroke="currentColor" strokeWidth="1.4" />
    <path
      d="M4.2 16.6c1.4-2.5 3.6-3.6 5.8-3.6s4.4 1.1 5.8 3.6"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
    />
  </svg>
);

const ISearch = (
  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
    <circle cx="9" cy="9" r="5" stroke="currentColor" strokeWidth="1.6" />
    <path d="M13 13l4 4" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
  </svg>
);

const IList = (
  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
    <path d="M5 6h12" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
    <path d="M5 10h12" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" opacity="0.8" />
    <path d="M5 14h12" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" opacity="0.6" />
    <circle cx="3" cy="6" r="1" fill="currentColor" />
    <circle cx="3" cy="10" r="1" fill="currentColor" opacity="0.8" />
    <circle cx="3" cy="14" r="1" fill="currentColor" opacity="0.6" />
  </svg>
);

const IStar = (
  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
    <path
      d="M10 3l2.2 4.6 5 .7-3.6 3.5.9 5-4.5-2.4-4.5 2.4.9-5L2.8 8.3l5-.7L10 3Z"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinejoin="round"
    />
  </svg>
);

// ===== Topbar icons =====
function FlagIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <path
        d="M5 17V3"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
      />
      <path
        d="M6 4.2h9l-1.2 3 1.2 3H6"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function BellIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <path
        d="M10 17.2c1.2 0 2.1-.8 2.4-1.8H7.6c.3 1 1.2 1.8 2.4 1.8Z"
        fill="currentColor"
        opacity="0.95"
      />
      <path
        d="M4.8 14.6h10.4c-1-1.1-1.2-2.2-1.2-4.6 0-2.6-1.6-4.3-4-4.8V4.4a1 1 0 1 0-2 0v.8c-2.4.5-4 2.2-4 4.8 0 2.4-.2 3.5-1.2 4.6Z"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function UserCircleIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <circle cx="10" cy="10" r="7" stroke="currentColor" strokeWidth="1.6" />
      <circle cx="10" cy="8" r="2.2" stroke="currentColor" strokeWidth="1.4" />
      <path
        d="M6.6 14.6c.9-1.4 2.1-2.1 3.4-2.1s2.5.7 3.4 2.1"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
      />
    </svg>
  );
}

function LogoutIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <path
        d="M8 6.2V5.3c0-1.3 1-2.3 2.3-2.3h4.4c1.3 0 2.3 1 2.3 2.3v9.4c0 1.3-1 2.3-2.3 2.3h-4.4C9 17 8 16 8 14.7v-.9"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
      />
      <path
        d="M10.2 10H3.3"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
      />
      <path
        d="M5.6 7.4 3 10l2.6 2.6"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function routeTitleFromNav(pathname, nav) {
  // choose the longest matching nav.to prefix
  let best = null;
  for (const item of nav) {
    if (!item?.to) continue;
    if (item.end) {
      if (pathname === item.to) best = item;
      continue;
    }
    if (pathname === item.to || pathname.startsWith(item.to + "/")) {
      if (!best || item.to.length > best.to.length) best = item;
    }
  }
  return best?.label || "Trang chủ";
}

export default function DashboardShell({ basePath, navItems }) {
  const navigate = useNavigate();
  const location = useLocation();

  const username = (localStorage.getItem("username") || "Người dùng").trim();
  const role = (localStorage.getItem("role") || "user").toLowerCase();
  const roleLabel = role === "admin" ? "Quản trị viên" : "Sinh viên";
  const brandSubtitle = role === "admin" ? "Data Management" : "Tra cứu tài liệu";
  const avatarLetter = (username[0] || "U").toUpperCase();

  function logout() {
    localStorage.removeItem("role");
    localStorage.removeItem("user_id");
    localStorage.removeItem("username");
    navigate("/login");
  }

  const subtitle = routeTitleFromNav(location.pathname, navItems);

  return (
    <div className="dash-shell">
      <aside className="dash-sidebar">
        <div className="dash-brand">
          <div className="brand-badge">SP</div>
          <div className="brand-lines">
            <div className="brand-title">Dashboard</div>
            <div className="brand-subtitle">{brandSubtitle}</div>
          </div>
        </div>

        <div className="dash-user">
          <div className="user-avatar">{avatarLetter}</div>
          <div className="user-lines">
            <div className="user-name">{username}</div>
            <div className="user-role">{roleLabel}</div>
          </div>
        </div>

        <nav className="dash-nav">
          <div className="nav-section-title">CHỨC NĂNG</div>

          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={Boolean(item.end)}
              className={({ isActive }) => (isActive ? "nav-item active" : "nav-item")}
            >
              <Icon>{item.icon}</Icon>
              <span className="nav-label">{item.label}</span>
            </NavLink>
          ))}
        </nav>

        <div className="dash-logout">
          <button className="logout-btn" onClick={logout} type="button">
            <LogoutIcon />
            <span>Đăng xuất</span>
          </button>
        </div>
      </aside>

      <main className="dash-main">
        <header className="dash-topbar">
          <div className="topbar-left">
            <div className="topbar-title">HỆ THỐNG QUẢN LÝ</div>
            <div className="topbar-subtitle">{subtitle}</div>
          </div>

          <div className="topbar-right">
            <button className="icon-btn" type="button" aria-label="Language">
              <FlagIcon />
            </button>
            <button className="icon-btn" type="button" aria-label="Notifications">
              <BellIcon />
              <span className="badge">6</span>
            </button>
            <button className="icon-btn" type="button" aria-label="Account">
              <UserCircleIcon />
            </button>
          </div>
        </header>

        <div className="dash-content">
          <Outlet />
        </div>
      </main>
    </div>
  );
}

// Export icons so Admin/User layouts can build nav configs
export const DashboardIcons = {
  IHome,
  IMinio,
  IDatabase,
  INeo4j,
  IUser,
  ISearch,
  IList,
  IStar,
};
