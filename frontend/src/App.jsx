import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";

import Login from "./pages/Login";
import RequireRole from "./components/RequireRole";

import AdminLayout from "./layouts/AdminLayout";
import UserLayout from "./layouts/UserLayout";

// Admin pages
// NOTE: các file admin đang dùng tên Home/MinIO/MongoDB/PostgreSQL (khác với import cũ)
import AdminHome from "./pages/admin/Home";
import MinioPage from "./pages/admin/MinIO";
import MongoPage from "./pages/admin/MongoDB";
import PostgresPage from "./pages/admin/PostgreSQL";
import Neo4jPage from "./pages/admin/Neo4j";
import UsersPage from "./pages/admin/Users";

// User pages
import UserHome from "./pages/user/UserHome";
import Search from "./pages/user/UserSearch";
import Library from "./pages/user/UserLibrary";
import Saved from "./pages/user/UserSaved";
import DocumentDetail from "./pages/user/UserDocDetail";
import DocumentView from "./pages/user/DocumentView";
import Profile from "./pages/user/UserProfile";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Navigate to="/login" replace />} />
        <Route path="/login" element={<Login />} />

        {/* Admin */}
        <Route
          path="/admin"
          element={
            <RequireRole allow="admin">
              <AdminLayout />
            </RequireRole>
          }
        >
          <Route index element={<AdminHome />} />
          <Route path="minio" element={<MinioPage />} />
          <Route path="mongo" element={<MongoPage />} />
          <Route path="postgres" element={<PostgresPage />} />
          <Route path="neo4j" element={<Neo4jPage />} />
          <Route path="users" element={<UsersPage />} />
        </Route>

        {/* User */}
        <Route
          path="/user"
          element={
            <RequireRole allow="user">
              <UserLayout />
            </RequireRole>
          }
        >
          <Route index element={<UserHome />} />
          <Route path="search" element={<Search />} />

          {/* Danh sách: giữ cả 2 route để tránh 404 khi bạn đổi tên */}
          <Route path="library" element={<Library />} />
          <Route path="user-library" element={<Library />} />

          <Route path="saved" element={<Saved />} />
          <Route path="docs/:chunkID" element={<DocumentDetail />} />
          <Route path="view/:chunkID" element={<DocumentView />} />
          <Route path="profile" element={<Profile />} />
        </Route>

        <Route path="*" element={<h1>404 - Not Found</h1>} />
      </Routes>
    </BrowserRouter>
  );
}
