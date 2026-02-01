import DashboardShell, { DashboardIcons } from "./DashboardShell";

export default function AdminLayout() {
  const { IHome, IMinio, IDatabase, INeo4j, IUser } = DashboardIcons;

  const navItems = [
    { to: "/admin", label: "Trang chủ", icon: IHome, end: true },
    { to: "/admin/minio", label: "MinIO", icon: IMinio },
    { to: "/admin/mongo", label: "MongoDB", icon: IDatabase },
    { to: "/admin/postgres", label: "PostgreSQL", icon: IDatabase },
    { to: "/admin/neo4j", label: "Neo4j", icon: INeo4j },
    { to: "/admin/users", label: "User", icon: IUser },
  ];

  return <DashboardShell basePath="/admin" navItems={navItems} />;
}
