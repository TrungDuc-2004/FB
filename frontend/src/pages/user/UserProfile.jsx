import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import "../../styles/user/profile.css";

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";
const AVATAR_STORAGE_KEY = "account_avatar";

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

function readAvatarStorage() {
  return localStorage.getItem(AVATAR_STORAGE_KEY) || "";
}

function getProfileAvatar(data, fallback = "") {
  return data?.avatar_url || data?.avatar_data_url || fallback || "";
}

function patchStoredProfile(next) {
  if (typeof next.username === "string") {
    localStorage.setItem("username", next.username);
  }
  if (typeof next.role === "string") {
    localStorage.setItem("role", next.role);
  }
  if (typeof next.userId === "string") {
    localStorage.setItem("user_id", next.userId);
  }
  if (typeof next.avatar === "string") {
    if (next.avatar) localStorage.setItem(AVATAR_STORAGE_KEY, next.avatar);
    else localStorage.removeItem(AVATAR_STORAGE_KEY);
  }

  window.dispatchEvent(new Event("account-profile-updated"));
}

export default function UserProfile() {
  const fileInputRef = useRef(null);
  const localPreviewUrlRef = useRef("");

  const initialProfile = useMemo(
    () => ({
      username: (localStorage.getItem("username") || "Người dùng").trim() || "Người dùng",
      role: (localStorage.getItem("role") || "user").toLowerCase(),
      userId: (localStorage.getItem("user_id") || "").trim(),
      avatar: readAvatarStorage(),
    }),
    []
  );

  const [sessionProfile, setSessionProfile] = useState(initialProfile);
  const [form, setForm] = useState({
    username: initialProfile.username,
    newPassword: "",
    confirmPassword: "",
    avatar: initialProfile.avatar,
  });
  const [meta, setMeta] = useState({
    role: initialProfile.role,
    userId: initialProfile.userId,
    isActive: true,
  });
  const [avatarFile, setAvatarFile] = useState(null);
  const [avatarRemoved, setAvatarRemoved] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

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
      setSessionProfile({
        username: nextUsername,
        role: nextRole,
        userId: nextUserId,
        avatar: nextAvatar,
      });
      patchStoredProfile({
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
    [revokeLocalPreview, sessionProfile.avatar, sessionProfile.role, sessionProfile.userId, sessionProfile.username]
  );

  useEffect(() => {
    return () => {
      revokeLocalPreview();
    };
  }, [revokeLocalPreview]);

  useEffect(() => {
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
          setError(String(err?.message || err || "Có lỗi xảy ra"));
        }
      })
      .finally(() => {
        if (!ignore) setIsLoading(false);
      });

    return () => {
      ignore = true;
    };
  }, [resetFormFromProfile, sessionProfile.userId, sessionProfile.username]);

  const changed = useMemo(() => {
    const nextName = form.username.trim();
    return Boolean(
      nextName !== (sessionProfile.username || "") || form.newPassword.trim() || avatarFile || avatarRemoved
    );
  }, [avatarFile, avatarRemoved, form.newPassword, form.username, sessionProfile.username]);

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

        setSessionProfile((prev) => ({
          ...prev,
          username: partialUsername,
          role: partialRole,
          userId: partialUserId,
        }));
        patchStoredProfile({
          username: partialUsername,
          role: partialRole,
          userId: partialUserId,
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
    <div className="user-profile-page">
      <div className="user-profile-head">
        <div>
          <h1>Tài khoản</h1>
          
        </div>
      </div>

      <div className="user-profile-body">
        <div className="user-profile-card">
          <div className="user-profile-top">
            <div className="user-profile-avatar">
              {form.avatar ? <img src={form.avatar} alt="Ảnh đại diện" /> : <span>{avatarLetter}</span>}
            </div>
            <div className="user-profile-name">{form.username || sessionProfile.username}</div>
            <div className="user-profile-role">{roleLabel}</div>
            <div className={`user-profile-status ${meta.isActive ? "active" : "inactive"}`}>
              {meta.isActive ? "Đang hoạt động" : "Đã vô hiệu hóa"}
            </div>
          </div>

          <div className="user-profile-avatar-actions">
            <input ref={fileInputRef} type="file" accept="image/*" hidden onChange={onAvatarFileChange} />
            <button className="user-profile-secondary-btn" type="button" onClick={triggerAvatarPicker}>
              <CameraIcon />
              <span>Đổi ảnh đại diện</span>
            </button>
            <button className="user-profile-ghost-btn" type="button" onClick={removeAvatar}>
              Xoá ảnh
            </button>
          </div>
        </div>

        <form className="user-profile-form" onSubmit={handleSubmit}>
          <div className="user-profile-grid">
            <label className="user-profile-field">
              <span>Tên tài khoản</span>
              <input
                value={form.username}
                onChange={(event) => updateField("username", event.target.value)}
                placeholder="Nhập tên tài khoản"
                disabled={isLoading || isSaving}
              />
            </label>

            <label className="user-profile-field">
              <span>Mã tài khoản</span>
              <input value={meta.userId || "Chưa có dữ liệu"} readOnly disabled />
            </label>

            <label className="user-profile-field">
              <span>Vai trò</span>
              <input value={roleLabel} readOnly disabled />
            </label>

            <label className="user-profile-field">
              <span>Trạng thái</span>
              <input value={meta.isActive ? "Đang hoạt động" : "Đã vô hiệu"} readOnly disabled />
            </label>
          </div>

          <div className="user-profile-grid password-grid">
            <label className="user-profile-field">
              <span>Mật khẩu mới</span>
              <input
                type="password"
                value={form.newPassword}
                onChange={(event) => updateField("newPassword", event.target.value)}
                placeholder="Bỏ trống nếu không đổi"
                disabled={isLoading || isSaving}
              />
            </label>

            <label className="user-profile-field">
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

          {error ? <div className="user-profile-message error">{error}</div> : null}
          {success ? <div className="user-profile-message success">{success}</div> : null}

          <div className="user-profile-actions">
            <button
              className="user-profile-ghost-btn"
              type="button"
              onClick={() => resetFormFromProfile(sessionProfile, { isActive: meta.isActive })}
              disabled={isSaving}
            >
              Hủy
            </button>
            <button className="user-profile-primary-btn" type="submit" disabled={isLoading || isSaving}>
              {isSaving ? "Đang lưu..." : "Xác nhận"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
