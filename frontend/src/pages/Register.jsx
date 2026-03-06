import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import "../styles/login.css";

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

export default function Register() {
  const navigate = useNavigate();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  async function handleSubmit(e) {
    e.preventDefault();
    setError("");
    setSuccess("");

    const u = username.trim();
    const pw = password.trim();
    const cpw = confirmPassword.trim();

    if (!u || !pw || !cpw) {
      setError("Vui lòng nhập đầy đủ thông tin");
      return;
    }
    if (pw.length < 6) {
      setError("Mật khẩu phải có ít nhất 6 ký tự");
      return;
    }
    if (pw !== cpw) {
      setError("Mật khẩu xác nhận không khớp");
      return;
    }

    setIsLoading(true);
    try {
      const res = await fetch(`${API_BASE}/admin/postgre/register`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          username: u,
          password: pw,
          confirm_password: cpw,
          user_role: "user",
          is_active: true,
        }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setError(data?.detail || "Đăng ký thất bại");
        return;
      }
      setSuccess("Đăng ký thành công. Tài khoản đã được lưu ở PostgreSQL và MongoDB.");
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
            <h2 className="form-title">Đăng ký tài khoản</h2>
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
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    className="input-control"
                    disabled={isLoading}
                  />
                </div>
                <div className="input-field">
                  <input
                    type="password"
                    placeholder="Xác nhận mật khẩu"
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
                {isLoading ? "Đang tạo tài khoản..." : "Đăng ký"}
              </button>
            </form>

            <div className="auth-links">
              <Link to="/login">Quay lại đăng nhập</Link>
              <Link to="/forgot-password">Quên mật khẩu</Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
