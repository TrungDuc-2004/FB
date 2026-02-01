import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";

import AdminLayout from "./layouts/AdminLayout";
import UserLayout from "./layouts/UserLayout";

import Login from "./pages/Login";
import RequireRole from "./components/RequireRole";

import Dashboard from "./pages/admin/Dashboard";
import MinIO from "./pages/admin/MinIO";
import MongoDB from "./pages/admin/MongoDB";
import PostgreSQL from "./pages/admin/PostgreSQL";
import Neo4j from "./pages/admin/Neo4j";
import Users from "./pages/admin/Users";

import UserHome from "./pages/user/UserHome";
import UserProfile from "./pages/user/UserProfile";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        {/* vào / thì chuyển sang /login */}
        <Route path="/" element={<Navigate to="/login" replace />} />

        <Route path="/login" element={<Login />} />

        {/* USER */}
        <Route
          path="/user"
          element={
            <RequireRole allow="user">
              <UserLayout />
            </RequireRole>
          }
        >
          <Route index element={<UserHome />} />

          {/* Student pages (UI giống admin, route dưới /user) */}
          <Route path="minio" element={<MinIO />} />
          <Route path="mongo" element={<MongoDB />} />
          <Route path="postgres" element={<PostgreSQL />} />
          <Route path="neo4j" element={<Neo4j />} />
          <Route path="profile" element={<UserProfile />} />
        </Route>

        {/* ADMIN */}
        <Route
          path="/admin"
          element={
            <RequireRole allow="admin">
              <AdminLayout />
            </RequireRole>
          }
        >
          {/* Trang chủ */}
          <Route index element={<Dashboard />} />

          <Route path="minio" element={<MinIO />} />
          <Route path="mongo" element={<MongoDB />} />
          <Route path="postgres" element={<PostgreSQL />} />
          <Route path="neo4j" element={<Neo4j />} />
          <Route path="users" element={<Users />} />
        </Route>
        <Route path="*" element={<h1>404 - Not Found</h1>} />
      </Routes>
    </BrowserRouter>
  );
}
