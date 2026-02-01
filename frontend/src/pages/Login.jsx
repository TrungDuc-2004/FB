// src/pages/Login.jsx
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import "../styles/login.css";

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

export default function Login() {
  const navigate = useNavigate();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  async function handleSubmit(e) {
    e.preventDefault();
    setError("");

    const u = username.trim();
    const pw = password.trim();

    if (!u || !pw) {
      setError("Vui lòng nhập đầy đủ thông tin");
      return;
    }

    setIsLoading(true);
    try {
      const res = await fetch(`${API_BASE}/admin/postgre/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: u, password: pw }),
      });

      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setError(data?.detail || "Đăng nhập thất bại");
        return;
      }

      // backend trả {user_id, username, role}
      localStorage.setItem("role", data.role || "user");
      localStorage.setItem("user_id", data.user_id || "");
      localStorage.setItem("username", data.username || u);

      if ((data.role || "").toLowerCase() === "admin") navigate("/admin");
      else navigate("/user");
    } catch (err) {
      setError(String(err?.message || err || "Network error"));
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <div className="login-page">
      {/* Background Image */}
      <div className="background-image"></div>

      <div className="login-container">
        <div className="login-card">
          {/* Header */}
          <div className="header">
            <div className="logo">
              <img src="/logo.png" alt="Logo trường" />
            </div>
            <div className="school-info">
              <h1 className="school-name">ĐH Sư phạm TP. Hồ Chí Minh</h1>
              <div className="school-subtitle">Khoá Luận Tốt Nghiệp</div>
            </div>
          </div>

          {/* Form */}
          <div className="form-container">
            <h2 className="form-title">Đăng nhập hệ thống</h2>

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
                    placeholder="Mật khẩu"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    className="input-control"
                    disabled={isLoading}
                  />
                </div>
              </div>

              {/* Error Message */}
              {error && (
                <div className="error-message">
                  <span className="error-icon">!</span>
                  <span>{error}</span>
                </div>
              )}

              {/* Submit Button */}
              <button type="submit" className="submit-btn" disabled={isLoading}>
                {isLoading ? "Đang đăng nhập..." : "Đăng nhập"}
              </button>
            </form>

            {/* Footer */}
          
          </div>
        </div>
      </div>
    </div>
  );
}
