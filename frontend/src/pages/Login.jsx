import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
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
    <div className="lp-root">
      <div className="lp-bg" aria-hidden="true" />
      <div className="lp-overlay" aria-hidden="true" />
      <div className="lp-blob lp-blob--1" aria-hidden="true" />
      <div className="lp-blob lp-blob--2" aria-hidden="true" />
      <div className="lp-blob lp-blob--3" aria-hidden="true" />

      <main className="lp-main">
        <section className="lp-card" aria-label="Đăng nhập">
          <header className="lp-header">
            <img
              className="lp-logo"
              src="/logo-hcmue.png"
              alt="Logo"
              onError={(e) => {
                e.currentTarget.src = "/logo.png";
              }}
            />
            <div>
              <h1 className="lp-title">ĐĂNG NHẬP</h1>
              <p className="lp-subtitle">Cổng thông tin đào tạo</p>
            </div>
          </header>

          <form className="lp-form" onSubmit={handleSubmit}>
            <label className="lp-label">
              <span className="lp-labelText">Tên đăng nhập</span>
              <div className="lp-inputWrap">
                <span className="lp-icon" aria-hidden="true">👤</span>
                <input
                  className="lp-input"
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  placeholder="Nhập tên đăng nhập"
                  autoComplete="username"
                  disabled={isLoading}
                  autoFocus
                />
              </div>
            </label>

            <label className="lp-label">
              <span className="lp-labelText">Mật khẩu</span>
              <div className="lp-inputWrap">
                <span className="lp-icon" aria-hidden="true">🔒</span>
                <input
                  className="lp-input"
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Nhập mật khẩu"
                  autoComplete="current-password"
                  disabled={isLoading}
                />
              </div>
            </label>

            {error ? (
              <div className="lp-alert err" role="alert">
                {error}
              </div>
            ) : null}

            <button className="lp-submit" type="submit" disabled={isLoading}>
              <span>{isLoading ? "Đang đăng nhập..." : "Đăng nhập"}</span>
            </button>

            <div className="lp-authLinks">
              <Link className="lp-link" to="/register">
                Đăng ký tài khoản
              </Link>
              <Link className="lp-link" to="/forgot-password">
                Quên mật khẩu
              </Link>
            </div>
          </form>
        </section>
      </main>
    </div>
  );
}
