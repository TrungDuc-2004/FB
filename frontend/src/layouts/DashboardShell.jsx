import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";
import "../styles/dashboard/layout.css";

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";
const AVATAR_STORAGE_KEY = "account_avatar";

function Icon({ children }) {
  return <span className="nav-icon">{children}</span>;
}

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

const IBook = (
  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
    <path
      d="M5.2 4.2A2.2 2.2 0 0 1 7.4 2h7.4v12.8H7.4a2.2 2.2 0 0 0-2.2 2.2V4.2Z"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinejoin="round"
    />
    <path d="M7.2 5.8h5.2" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
    <path d="M7.2 8.8h5.2" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" opacity="0.75" />
    <path d="M5.2 17c0-.9.7-1.6 1.6-1.6h8" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
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
      <path d="M10.2 10H3.3" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
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

function CameraIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <path
        d="M6.2 5.4 7.1 4h5.8l.9 1.4h1.5c1.1 0 2 .9 2 2v6.1c0 1.1-.9 2-2 2H4.7c-1.1 0-2-.9-2-2V7.4c0-1.1.9-2 2-2h1.5Z"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinejoin="round"
      />
      <circle cx="10" cy="10.4" r="2.8" stroke="currentColor" strokeWidth="1.4" />
    </svg>
  );
}

function routeTitleFromNav(pathname, nav) {
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
  return best?.label || nav?.[0]?.label || "Dashboard";
}

function readAvatarStorage() {
  return localStorage.getItem(AVATAR_STORAGE_KEY) || "";
}

function getProfileAvatar(data, fallback = "") {
  return data?.avatar_url || data?.avatar_data_url || fallback || "";
}

function useSessionProfile() {
  const [profile, setProfile] = useState(() => ({
    username: (localStorage.getItem("username") || "Người dùng").trim(),
    role: (localStorage.getItem("role") || "user").toLowerCase(),
    userId: (localStorage.getItem("user_id") || "").trim(),
    avatar: readAvatarStorage(),
  }));

  const patchProfile = useCallback((next) => {
    setProfile((prev) => {
      const merged = { ...prev, ...next };
      if (typeof merged.username === "string") {
        localStorage.setItem("username", merged.username);
      }
      if (typeof merged.role === "string") {
        localStorage.setItem("role", merged.role);
      }
      if (typeof merged.userId === "string") {
        localStorage.setItem("user_id", merged.userId);
      }
      if (typeof merged.avatar === "string") {
        if (merged.avatar) localStorage.setItem(AVATAR_STORAGE_KEY, merged.avatar);
        else localStorage.removeItem(AVATAR_STORAGE_KEY);
      }
      return merged;
    });
  }, []);

  return [profile, patchProfile];
}

function AccountModal({
  open,
  onClose,
  onLogout,
  sessionProfile,
  patchSessionProfile,
  title,
}) {
  const [form, setForm] = useState({
    username: sessionProfile.username,
    newPassword: "",
    confirmPassword: "",
    avatar: sessionProfile.avatar,
  });
  const [meta, setMeta] = useState({
    role: sessionProfile.role,
    userId: sessionProfile.userId,
    isActive: true,
  });
  const [avatarFile, setAvatarFile] = useState(null);
  const [avatarRemoved, setAvatarRemoved] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const fileInputRef = useRef(null);
  const localPreviewUrlRef = useRef("");

  const roleLabel = meta.role === "admin" ? "Quản trị viên" : "Sinh viên";
  const avatarLetter = (form.username?.trim()[0] || sessionProfile.username?.trim()[0] || "U").toUpperCase();

  const revokeLocalPreview = useCallback(() => {
    if (localPreviewUrlRef.current) {
      URL.revokeObjectURL(localPreviewUrlRef.current);
      localPreviewUrlRef.current = "";
    }
  }, []);

  const resetFormFromProfile = useCallback(
    (data, fallback = {}) => {
      const nextUsername = data?.username || fallback.username || sessionProfile.username;
      const nextRole = (data?.role || fallback.role || sessionProfile.role || "user").toLowerCase();
      const nextUserId = data?.user_id || fallback.userId || sessionProfile.userId || "";
      const nextAvatar = getProfileAvatar(data, fallback.avatar || sessionProfile.avatar || "");
      const nextIsActive = data?.is_active ?? fallback.isActive ?? true;

      revokeLocalPreview();
      if (fileInputRef.current) fileInputRef.current.value = "";
      setAvatarFile(null);
      setAvatarRemoved(false);
      patchSessionProfile({
        username: nextUsername,
        role: nextRole,
        userId: nextUserId,
        avatar: nextAvatar,
      });
      setForm({
        username: nextUsername,
        newPassword: "",
        confirmPassword: "",
        avatar: nextAvatar,
      });
      setMeta({
        role: nextRole,
        userId: nextUserId,
        isActive: nextIsActive,
      });
    },
    [patchSessionProfile, revokeLocalPreview, sessionProfile.avatar, sessionProfile.role, sessionProfile.userId, sessionProfile.username]
  );

  useEffect(() => {
    return () => {
      revokeLocalPreview();
    };
  }, [revokeLocalPreview]);

  useEffect(() => {
    if (!open) return;

    let ignore = false;
    setError("");
    setSuccess("");
    setIsLoading(true);

    async function loadProfile() {
      const params = new URLSearchParams();
      if (sessionProfile.userId) params.set("user_id", sessionProfile.userId);
      else if (sessionProfile.username) params.set("username", sessionProfile.username);

      const res = await fetch(`${API_BASE}/admin/postgre/profile?${params.toString()}`);
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data?.detail || "Không tải được thông tin tài khoản");
      }
      if (ignore) return;

      resetFormFromProfile(data);
    }

    loadProfile()
      .catch((err) => {
        if (!ignore) {
          revokeLocalPreview();
          if (fileInputRef.current) fileInputRef.current.value = "";
          setAvatarFile(null);
          setAvatarRemoved(false);
          setForm({
            username: sessionProfile.username,
            newPassword: "",
            confirmPassword: "",
            avatar: sessionProfile.avatar,
          });
          setMeta({
            role: sessionProfile.role,
            userId: sessionProfile.userId,
            isActive: true,
          });
          setError(String(err?.message || err || "Có lỗi xảy ra"));
        }
      })
      .finally(() => {
        if (!ignore) setIsLoading(false);
      });

    return () => {
      ignore = true;
    };
  }, [
    open,
    resetFormFromProfile,
    revokeLocalPreview,
    sessionProfile.avatar,
    sessionProfile.role,
    sessionProfile.userId,
    sessionProfile.username,
  ]);

  useEffect(() => {
    if (!open) return;
    function handleEscape(event) {
      if (event.key === "Escape") onClose();
    }
    document.addEventListener("keydown", handleEscape);
    return () => document.removeEventListener("keydown", handleEscape);
  }, [open, onClose]);

  const changed = useMemo(() => {
    const nextName = form.username.trim();
    return Boolean(
      nextName !== (sessionProfile.username || "") ||
        form.newPassword.trim() ||
        avatarFile ||
        avatarRemoved
    );
  }, [avatarFile, avatarRemoved, form.newPassword, form.username, sessionProfile.username]);

  if (!open) return null;

  function updateField(key, value) {
    setForm((prev) => ({ ...prev, [key]: value }));
  }

  function triggerAvatarPicker() {
    fileInputRef.current?.click();
  }

  function onAvatarFileChange(event) {
    const file = event.target.files?.[0];
    if (!file) return;

    if (!file.type.startsWith("image/")) {
      setError("Vui lòng chọn file ảnh.");
      return;
    }

    revokeLocalPreview();
    const previewUrl = URL.createObjectURL(file);
    localPreviewUrlRef.current = previewUrl;
    setAvatarFile(file);
    setAvatarRemoved(false);
    updateField("avatar", previewUrl);
    setError("");
    setSuccess("");
  }

  function removeAvatar() {
    revokeLocalPreview();
    setAvatarFile(null);
    setAvatarRemoved(Boolean(sessionProfile.avatar || form.avatar));
    updateField("avatar", "");
    if (fileInputRef.current) fileInputRef.current.value = "";
    setError("");
    setSuccess("");
  }

  async function parseJsonResponse(response, fallbackMessage) {
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data?.detail || fallbackMessage);
    }
    return data;
  }

  async function handleSubmit(event) {
    event?.preventDefault?.();
    setError("");
    setSuccess("");

    const nextUsername = form.username.trim();
    const nextPassword = form.newPassword.trim();
    const confirmPassword = form.confirmPassword.trim();

    if (!nextUsername) {
      setError("Tên tài khoản không được để trống.");
      return;
    }

    if (nextPassword && nextPassword.length < 6) {
      setError("Mật khẩu mới phải có ít nhất 6 ký tự.");
      return;
    }

    if (nextPassword !== confirmPassword) {
      setError("Mật khẩu xác nhận không khớp.");
      return;
    }

    if (!changed) {
      setSuccess("Chưa có thay đổi nào để lưu.");
      return;
    }

    setIsSaving(true);
    let textProfile = null;

    try {
      const profilePayload = {
        user_id: meta.userId || sessionProfile.userId,
        current_username: sessionProfile.username,
        new_username: nextUsername,
        new_password: nextPassword || null,
        confirm_password: nextPassword ? confirmPassword : null,
      };

      const profileRes = await fetch(`${API_BASE}/admin/postgre/profile`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(profilePayload),
      });
      textProfile = await parseJsonResponse(profileRes, "Cập nhật tài khoản thất bại");

      const resolvedUserId = textProfile.user_id || meta.userId || sessionProfile.userId || "";
      const resolvedUsername = textProfile.username || nextUsername;
      let finalProfile = textProfile;

      if (avatarFile) {
        const avatarBody = new FormData();
        if (resolvedUserId) avatarBody.append("user_id", resolvedUserId);
        avatarBody.append("current_username", resolvedUsername);
        avatarBody.append("file", avatarFile);

        const avatarRes = await fetch(`${API_BASE}/admin/postgre/profile/avatar`, {
          method: "POST",
          body: avatarBody,
        });
        finalProfile = await parseJsonResponse(avatarRes, "Tải ảnh đại diện thất bại");
      } else if (avatarRemoved && (sessionProfile.avatar || getProfileAvatar(textProfile))) {
        const params = new URLSearchParams();
        if (resolvedUserId) params.set("user_id", resolvedUserId);
        else params.set("username", resolvedUsername);

        const deleteRes = await fetch(`${API_BASE}/admin/postgre/profile/avatar?${params.toString()}`, {
          method: "DELETE",
        });
        finalProfile = await parseJsonResponse(deleteRes, "Xoá ảnh đại diện thất bại");
      }

      resetFormFromProfile(finalProfile, {
        username: resolvedUsername,
        role: textProfile.role || meta.role,
        userId: resolvedUserId,
        isActive: textProfile.is_active ?? meta.isActive,
        avatar: getProfileAvatar(finalProfile),
      });
      setSuccess("Đã cập nhật thông tin tài khoản.");
    } catch (err) {
      if (textProfile) {
        const partialUsername = textProfile.username || nextUsername;
        const partialRole = (textProfile.role || meta.role || "user").toLowerCase();
        const partialUserId = textProfile.user_id || meta.userId || sessionProfile.userId || "";

        patchSessionProfile({
          username: partialUsername,
          role: partialRole,
          userId: partialUserId,
          avatar: sessionProfile.avatar || "",
        });
        setForm((prev) => ({
          ...prev,
          username: partialUsername,
          newPassword: "",
          confirmPassword: "",
        }));
        setMeta((prev) => ({
          ...prev,
          role: partialRole,
          userId: partialUserId,
          isActive: textProfile.is_active ?? prev.isActive,
        }));
        setError(`Đã cập nhật tên hoặc mật khẩu, nhưng phần ảnh đại diện chưa thành công: ${String(err?.message || err || "Có lỗi xảy ra")}`);
      } else {
        setError(String(err?.message || err || "Có lỗi xảy ra"));
      }
    } finally {
      setIsSaving(false);
    }
  }

  return (
    <div className="account-modal-backdrop" onClick={onClose}>
      <div
        className="account-modal"
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="account-modal-header">
          <div>
            <div className="account-modal-title">{title}</div>
          </div>
          <button className="account-modal-close" type="button" onClick={onClose}>
            ×
          </button>
        </div>

        <div className="account-modal-body">
          <div className="account-profile-card">
            <div className="account-profile-top">
              <div className="account-avatar account-avatar-large">
                {form.avatar ? <img src={form.avatar} alt="Ảnh đại diện" /> : <span>{avatarLetter}</span>}
              </div>
              <div className="account-profile-lines">
                <div className="account-name">{form.username || sessionProfile.username}</div>
                <div className="account-role">{roleLabel}</div>
                <div className={`account-status ${meta.isActive ? "active" : "inactive"}`}>
                  {meta.isActive ? "Đang hoạt động" : "Đã vô hiệu hóa"}
                </div>
              </div>
            </div>

            <div className="account-avatar-actions">
              <input
                ref={fileInputRef}
                type="file"
                accept="image/*"
                hidden
                onChange={onAvatarFileChange}
              />
              <button className="account-secondary-btn" type="button" onClick={triggerAvatarPicker}>
                <CameraIcon />
                <span>Đổi ảnh đại diện</span>
              </button>
              <button className="account-ghost-btn" type="button" onClick={removeAvatar}>
                Xoá ảnh
              </button>
            </div>
          </div>

          <form className="account-form" onSubmit={handleSubmit}>
            <div className="account-grid">
              <label className="account-field">
                <span>Tên tài khoản</span>
                <input
                  value={form.username}
                  onChange={(event) => updateField("username", event.target.value)}
                  placeholder="Nhập tên tài khoản"
                  disabled={isLoading || isSaving}
                />
              </label>

              <label className="account-field">
                <span>Mã tài khoản</span>
                <input value={meta.userId || "Chưa có dữ liệu"} readOnly disabled />
              </label>

              <label className="account-field">
                <span>Vai trò</span>
                <input value={roleLabel} readOnly disabled />
              </label>

              <label className="account-field">
                <span>Trạng thái</span>
                <input value={meta.isActive ? "Active" : "Disabled"} readOnly disabled />
              </label>
            </div>

            <div className="account-grid password-grid">
              <label className="account-field">
                <span>Mật khẩu mới</span>
                <input
                  type="password"
                  value={form.newPassword}
                  onChange={(event) => updateField("newPassword", event.target.value)}
                  placeholder="Bỏ trống nếu không đổi"
                  disabled={isLoading || isSaving}
                />
              </label>

              <label className="account-field">
                <span>Xác nhận mật khẩu</span>
                <input
                  type="password"
                  value={form.confirmPassword}
                  onChange={(event) => updateField("confirmPassword", event.target.value)}
                  placeholder="Nhập lại mật khẩu mới"
                  disabled={isLoading || isSaving}
                />
              </label>
            </div>

            {error ? <div className="account-message error">{error}</div> : null}
            {success ? <div className="account-message success">{success}</div> : null}

            <div className="account-actions">
              <button className="account-ghost-btn" type="button" onClick={onClose} disabled={isSaving}>
                Đóng
              </button>
              <button className="account-primary-btn" type="submit" disabled={isLoading || isSaving}>
                {isSaving ? "Đang lưu..." : "Lưu thay đổi"}
              </button>
            </div>
          </form>
        </div>

        <div className="account-footer">
          <button className="logout-btn account-logout-btn" type="button" onClick={onLogout}>
            <LogoutIcon />
            <span>Đăng xuất</span>
          </button>
        </div>
      </div>
    </div>
  );
}

export default function DashboardShell({
  navItems,
  brandLogoSrc,
  brandTitle = "Dashboard",
  brandSubtitle,
  showSidebarUser = true,
  topbarTitle = "HỆ THỐNG QUẢN LÝ",
  accountInfoTitle = "Thông tin tài khoản",
  shellClassName = "",
}) {

  const navigate = useNavigate();
  const location = useLocation();
  const [accountOpen, setAccountOpen] = useState(false);
  const [sessionProfile, patchSessionProfile] = useSessionProfile();

  const username = sessionProfile.username;
  const role = sessionProfile.role;
  const roleLabel = role === "admin" ? "Quản trị viên" : "Sinh viên";
  const avatarLetter = (username[0] || "U").toUpperCase();

  const resolvedBrandSubtitle =
    brandSubtitle || (role === "admin" ? "Data Management" : "Tra cứu tài liệu");
  const subtitle = routeTitleFromNav(location.pathname, navItems);

  function logout() {
    localStorage.removeItem("role");
    localStorage.removeItem("user_id");
    localStorage.removeItem("username");
    localStorage.removeItem(AVATAR_STORAGE_KEY);
    navigate("/login");
  }

  function openAccountModal() {
    setAccountOpen(true);
  }

  function closeAccountModal() {
    setAccountOpen(false);
  }

  return (
    <div className={`dash-shell ${shellClassName}`.trim()}>
      <aside className="dash-sidebar">
        <div className="dash-brand">
          {brandLogoSrc ? (
            <img className="brand-logo" src={brandLogoSrc} alt="Logo đơn vị" />
          ) : (
            <div className="brand-badge">SP</div>
          )}

          <div className="brand-lines">
            <div className="brand-title">{brandTitle}</div>
            {resolvedBrandSubtitle ? (
              <div className="brand-subtitle">{resolvedBrandSubtitle}</div>
            ) : null}
          </div>
        </div>

        {showSidebarUser ? (
          <div className="dash-user">
            <div className="user-avatar">
              {sessionProfile.avatar ? (
                <img className="user-avatar-image" src={sessionProfile.avatar} alt="Ảnh đại diện" />
              ) : (
                avatarLetter
              )}
            </div>
            <div className="user-lines">
              <div className="user-name">{username}</div>
              <div className="user-role">{roleLabel}</div>
            </div>
          </div>
        ) : null}

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
            <div className="topbar-title">{topbarTitle}</div>
            <div className="topbar-subtitle">{subtitle}</div>
          </div>

          <div className="topbar-right">
            <button
              className={`icon-btn account-btn ${accountOpen ? "open" : ""}`}
              type="button"
              aria-label="Tài khoản"
              aria-expanded={accountOpen}
              onClick={openAccountModal}
            >
              <UserCircleIcon />
            </button>
          </div>
        </header>

        <div className="dash-content">
          <Outlet />
        </div>
      </main>

      <AccountModal
        open={accountOpen}
        onClose={closeAccountModal}
        onLogout={logout}
        sessionProfile={sessionProfile}
        patchSessionProfile={patchSessionProfile}
        title={accountInfoTitle}
      />
    </div>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export const DashboardIcons = {
  IHome,
  IMinio,
  IDatabase,
  INeo4j,
  IUser,
  ISearch,
  IList,
  IBook,
  IStar,
};
