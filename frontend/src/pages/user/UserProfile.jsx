export default function UserProfile() {
  const username = localStorage.getItem("username") || "Người dùng";
  const role = (localStorage.getItem("role") || "user").toLowerCase();
  const roleLabel = role === "admin" ? "Quản trị viên" : "Sinh viên";

  return (
    <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 16, padding: 18 }}>
      <h2 style={{ margin: 0, fontSize: 20 }}>Thông tin tài khoản</h2>
      <div style={{ marginTop: 12, lineHeight: 1.8 }}>
        <div>
          <strong>Họ tên / Username:</strong> {username}
        </div>
        <div>
          <strong>Vai trò:</strong> {roleLabel}
        </div>
      </div>
    </div>
  );
}
