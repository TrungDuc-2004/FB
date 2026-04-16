import DashboardShell, { DashboardIcons } from "./DashboardShell";

export default function AdminLayout() {
  const { IMinio, IDatabase, INeo4j, IUser } = DashboardIcons;

  const navItems = [
    { to: "/admin/minio", label: "Dữ liệu đối tượng", icon: IMinio },
    { to: "/admin/mongo", label: "Dữ liệu mô tả", icon: IDatabase },
    { to: "/admin/postgres", label: "Dữ liệu có cấu trúc", icon: IDatabase },
    { to: "/admin/neo4j", label: "Dữ liệu đồ thị", icon: INeo4j },
    { to: "/admin/users", label: "Tài khoản", icon: IUser },
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
      shellClassName="admin-shell"
    />
  );
}
