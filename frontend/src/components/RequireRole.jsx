// components/RequireRole.jsx
import { Navigate } from "react-router-dom";

export default function RequireRole({ allow, children }) {
  const role = localStorage.getItem("role");

  // chưa login
  if (!role) return <Navigate to="/login" replace />;

  // role không đúng
  if (allow && role !== allow) {
    return <Navigate to="/login" replace />;
  }

  return children;
}
