import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import "../styles/login.css";

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

export default function ForgotPassword() {
  const navigate = useNavigate();
  const [username, setUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  async function handleSubmit(e) {
    e.preventDefault();
    setError("");
    setSuccess("");

    const u = username.trim();
    const pw = newPassword.trim();
    const cpw = confirmPassword.trim();

    if (!u || !pw || !cpw) {
      setError("Vui lòng nhập đầy đủ thông tin");
      return;
    }
    if (pw.length < 6) {
      setError("Mật khẩu mới phải có ít nhất 6 ký tự");
      return;
    }
    if (pw !== cpw) {
      setError("Mật khẩu xác nhận không khớp");
      return;
    }

    setIsLoading(true);
    try {
      const res = await fetch(`${API_BASE}/admin/postgre/forgot-password`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          username: u,
          new_password: pw,
          confirm_password: cpw,
        }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setError(data?.detail || "Đổi mật khẩu thất bại");
        return;
      }
      setSuccess("Đổi mật khẩu thành công. Dữ liệu đã được cập nhật ở PostgreSQL và MongoDB.");
      setTimeout(() => navigate("/login"), 1200);
    } catch (err) {
      setError(String(err?.message || err || "Network error"));
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <div className="login-page">
      <div className="background-image"></div>
      <div className="login-container auth-container-wide">
        <div className="login-card">
          <div className="header">
            <div className="logo">
              <img src="/logo.png" alt="Logo trường" />
            </div>
            <div className="school-info">
              <h1 className="school-name">ĐH Sư phạm TP. Hồ Chí Minh</h1>
              <div className="school-subtitle">Khoá Luận Tốt Nghiệp</div>
            </div>
          </div>

          <div className="form-container">
            <h2 className="form-title">Quên mật khẩu</h2>
            <form className="login-form" onSubmit={handleSubmit}>
              <div className="input-group">
                <div className="input-field">
                  <input
                    type="text"
                    placeholder="Tên đăng nhập"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    className="input-control"
                    disabled={isLoading}
                    autoFocus
                  />
                </div>
                <div className="input-field">
                  <input
                    type="password"
                    placeholder="Mật khẩu mới"
                    value={newPassword}
                    onChange={(e) => setNewPassword(e.target.value)}
                    className="input-control"
                    disabled={isLoading}
                  />
                </div>
                <div className="input-field">
                  <input
                    type="password"
                    placeholder="Xác nhận mật khẩu mới"
                    value={confirmPassword}
                    onChange={(e) => setConfirmPassword(e.target.value)}
                    className="input-control"
                    disabled={isLoading}
                  />
                </div>
              </div>

              {error && (
                <div className="error-message">
                  <span className="error-icon">!</span>
                  <span>{error}</span>
                </div>
              )}
              {success && <div className="success-message">{success}</div>}

              <button type="submit" className="submit-btn" disabled={isLoading}>
                {isLoading ? "Đang cập nhật..." : "Đặt lại mật khẩu"}
              </button>
            </form>

            <div className="auth-links">
              <Link to="/login">Quay lại đăng nhập</Link>
              <Link to="/register">Tạo tài khoản mới</Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
