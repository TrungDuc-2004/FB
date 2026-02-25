import DashboardShell, { DashboardIcons } from "./DashboardShell";

export default function UserLayout() {
  const { IHome, IBook, ISearch, IStar, IUser } = DashboardIcons;

  // User UI: chỉ giữ các chức năng tra cứu/tài liệu
  const navItems = [
    { to: "/user", label: "Trang chủ", icon: IHome, end: true },
    { to: "/user/library", label: "Danh sách", icon: IBook },
    { to: "/user/search", label: "Tìm kiếm", icon: ISearch },
    { to: "/user/saved", label: "Đã lưu", icon: IStar },
    { to: "/user/profile", label: "User", icon: IUser },
  ];

  return <DashboardShell basePath="/user" navItems={navItems} />;
}
