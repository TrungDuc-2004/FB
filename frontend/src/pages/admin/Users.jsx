// pages/admin/Users.jsx
import { useEffect, useMemo, useState } from "react";
import "../../styles/admin/page.css";
import "../../styles/admin/modal.css";
import DataTable from "../../components/DataTable";
import * as userApi from "../../services/userMongoApi";

function nowStr() {
  return new Date().toISOString().slice(0, 16).replace("T", " ");
}

function fmtTime(s) {
  // Mongo jsonable_encoder thường ra ISO: 2026-01-27T09:00:00+00:00
  if (!s) return "";
  const str = String(s);
  if (str.includes("T")) return str.slice(0, 16).replace("T", " ");
  return str.slice(0, 16);
}

function UserModal({ open, onClose, title, initial, onSave }) {
  const [username, setUsername] = useState(initial?.username || "");
  const [password, setPassword] = useState(""); // edit cũng bắt nhập lại
  const [role, setRole] = useState(initial?.role || "user");
  const [active, setActive] = useState(initial?.active ?? true);

  useEffect(() => {
    if (!open) return;
    setUsername(initial?.username || "");
    setPassword("");
    setRole(initial?.role || "user");
    setActive(initial?.active ?? true);
  }, [open, initial]);

  if (!open) return null;

  function submit(e) {
    e?.preventDefault?.();
    const u = username.trim();
    const pw = password.trim();
    if (!u) return;

    if (!pw) {
      alert("Vui lòng nhập password!");
      return;
    }

    onSave({ username: u, password: pw, role, active });
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="modal-title">{title}</h3>
          <button className="modal-close" onClick={onClose}>
            ×
          </button>
        </div>

        <div className="modal-body">
          <form onSubmit={submit}>
            <div className="field">
              <label>User name</label>
              <input value={username} onChange={(e) => setUsername(e.target.value)} autoFocus />
            </div>

            <div className="field">
              <label>Password</label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Nhập mật khẩu..."
              />
            </div>

            <div className="field">
              <label>Role</label>
              <select value={role} onChange={(e) => setRole(e.target.value)}>
                <option value="user">User</option>
                <option value="admin">Admin</option>
              </select>
            </div>

            <div className="field" style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <input
                id="active"
                type="checkbox"
                checked={active}
                onChange={(e) => setActive(e.target.checked)}
                style={{ width: 18, height: 18 }}
              />
              <label htmlFor="active" style={{ margin: 0 }}>
                Kích hoạt tài khoản
              </label>
            </div>
          </form>
        </div>

        <div className="modal-footer">
          <button className="btn" onClick={onClose}>
            Huỷ bỏ
          </button>
          <button className="btn btn-primary" onClick={submit}>
            Cập nhật
          </button>
        </div>
      </div>
    </div>
  );
}

export default function Users() {
  const [q, setQ] = useState("");

  // ✅ data từ Mongo
  const [users, setUsers] = useState([]); // {id,username,role,active,updatedAt}

  // modal
  const [openCreate, setOpenCreate] = useState(false);
  const [openEdit, setOpenEdit] = useState(false);
  const [editTarget, setEditTarget] = useState(null);

  async function reloadUsers() {
    const data = await userApi.listUsers({ limit: 500, offset: 0 });
    const docs = data.documents || [];

    const mapped = docs.map((d) => ({
      id: String(d._id),
      username: d.username || "",
      role: d.user_role || "user",
      active: d.is_active ?? true,
      updatedAt: fmtTime(d.updated_at || d.created_at || ""),
      // không hiển thị password ra table
    }));

    setUsers(mapped);
  }

  useEffect(() => {
    reloadUsers().catch((e) => {
      console.error(e);
      alert(`Load users failed: ${e.message || e}`);
    });
  }, []);

  const rows = useMemo(() => {
    const s = q.trim().toLowerCase();
    const list = !s
      ? users
      : users.filter(
          (u) =>
            (u.username || "").toLowerCase().includes(s) ||
            (u.role || "").toLowerCase().includes(s) ||
            (u.id || "").toLowerCase().includes(s)
        );

    return list
      .slice()
      .sort((a, b) => (b.updatedAt || "").localeCompare(a.updatedAt || ""));
  }, [users, q]);

  const columns = [
    {
      key: "id",
      label: "USER_ID",
      render: (r) => <span className="crumb">{r.id}</span>,
    },
    {
      key: "username",
      label: "USER_NAME",
      render: (r) => (
        <div className="file-cell">
          <div className="file-left">
            <div className={`file-icon ${r.active ? "file-other" : "file-pdf"}`}>
              {r.active ? "👤" : "🚫"}
            </div>
            <div className="file-divider" />
            <div className="file-name" title={r.username}>
              {r.username}
            </div>
          </div>
        </div>
      ),
    },
    {
      key: "role",
      label: "ROLE",
      render: (r) => (
        <span className={`role-badge ${r.role === "admin" ? "is-admin" : "is-user"}`}>
          {r.role === "admin" ? "Admin" : "User"}
        </span>
      ),
    },
    {
      key: "status",
      label: "TRẠNG THÁI",
      render: (r) => (
        <span className={`status-badge ${r.active ? "is-active" : "is-disabled"}`}>
          {r.active ? "Active" : "Disabled"}
        </span>
      ),
    },
    { key: "updatedAt", label: "CẬP NHẬT" },
  ];

  async function toggleDisable(row) {
    const nextActive = !row.active;
    if (!confirm(`${nextActive ? "Kích hoạt" : "Vô hiệu hoá"} tài khoản "${row.username}"?`)) return;

    try {
      await userApi.updateUser(row.id, { is_active: nextActive });
      await reloadUsers();
    } catch (e) {
      console.error(e);
      alert(`Update failed: ${e.message || e}`);
    }
  }

  function openEditUser(row) {
    setEditTarget(row);
    setOpenEdit(true);
  }

  async function saveEditUser(data) {
    if (!editTarget) return;

    try {
      await userApi.updateUser(editTarget.id, {
        username: data.username,
        password: data.password,
        user_role: data.role,
        is_active: data.active,
      });
      await reloadUsers();
      setOpenEdit(false);
      setEditTarget(null);
    } catch (e) {
      console.error(e);
      alert(`Update failed: ${e.message || e}`);
    }
  }

  async function saveCreateUser(data) {
    try {
      await userApi.createUser({
        username: data.username,
        password: data.password,
        user_role: data.role,
        is_active: data.active,
      });
      await reloadUsers();
      setOpenCreate(false);
    } catch (e) {
      console.error(e);
      alert(`Create failed: ${e.message || e}`);
    }
  }

  return (
    <div>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <h2 className="page-title">Users</h2>
          </div>
        </div>

        <div className="page-header-bottom">
          <div className="search-box">
            <input
              placeholder="Tìm user (id / username / role)..."
              value={q}
              onChange={(e) => setQ(e.target.value)}
            />
          </div>

          <div className="header-actions">
            <button className="btn btn-primary" onClick={() => setOpenCreate(true)}>
              + Thêm User
            </button>
          </div>
        </div>
      </div>

      <div className="table-wrapper">
        <DataTable
          pageSize={7}
          columns={columns}
          rows={rows}
          getRowClassName={() => "row-click"}
          renderActions={(row) => (
            <div className="table-actions" onDoubleClick={(e) => e.stopPropagation()}>
              <button
                className={`btn ${row.active ? "btn-danger" : "btn-success"}`}
                onClick={(e) => {
                  e.stopPropagation();
                  toggleDisable(row);
                }}
              >
                {row.active ? "Vô hiệu" : "Kích hoạt"}
              </button>

              <button
                className="btn"
                onClick={(e) => {
                  e.stopPropagation();
                  openEditUser(row);
                }}
              >
                Sửa
              </button>
            </div>
          )}
        />
      </div>

      <UserModal
        open={openCreate}
        onClose={() => setOpenCreate(false)}
        title="Thêm User"
        initial={{ username: "", role: "user", active: true }}
        onSave={saveCreateUser}
      />

      <UserModal
        open={openEdit}
        onClose={() => {
          setOpenEdit(false);
          setEditTarget(null);
        }}
        title="Sửa User"
        initial={editTarget}
        onSave={saveEditUser}
      />
    </div>
  );
}
