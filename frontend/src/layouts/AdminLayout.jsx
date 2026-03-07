import DashboardShell, { DashboardIcons } from "./DashboardShell";

export default function AdminLayout() {
  const { IMinio, IDatabase, INeo4j, IUser } = DashboardIcons;

  const navItems = [
    { to: "/admin/minio", label: "MinIO", icon: IMinio },
    { to: "/admin/mongo", label: "MongoDB", icon: IDatabase },
    { to: "/admin/postgres", label: "PostgreSQL", icon: IDatabase },
    { to: "/admin/neo4j", label: "Neo4j", icon: INeo4j },
    { to: "/admin/users", label: "User", icon: IUser },
  ];

  return (
    <DashboardShell
      basePath="/admin"
      navItems={navItems}
      brandLogoSrc="/logo.png"
      brandTitle="Đại học Sư phạm TP HCM"
      brandSubtitle="Hệ thống quản trị"
      showSidebarUser={false}
      topbarTitle="TRANG QUẢN TRỊ"
      accountInfoTitle="Tài khoản"
    />
  );
}
