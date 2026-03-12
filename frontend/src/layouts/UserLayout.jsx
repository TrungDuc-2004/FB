import { useEffect } from "react";
import DashboardShell, { DashboardIcons } from "./DashboardShell";

export default function UserLayout() {
  const { IHome, IBook, ISearch, IStar, IUser } = DashboardIcons;

  useEffect(() => {
    document.body.classList.add("user-shell-clean");
    return () => {
      document.body.classList.remove("user-shell-clean");
    };
  }, []);

  const navItems = [
    { to: "/user", label: "Trang chủ", icon: IHome, end: true },
    { to: "/user/library", label: "Danh sách", icon: IBook },
    { to: "/user/search", label: "Tìm kiếm", icon: ISearch },
    { to: "/user/saved", label: "Đã lưu", icon: IStar },
    { to: "/user/profile", label: "Tài khoản", icon: IUser },
  ];

  return (
    <DashboardShell
      navItems={navItems}
      brandTitle="Dashboard"
      brandSubtitle="Tra cứu tài liệu"
      topbarTitle="HỆ THỐNG QUẢN LÝ"
      accountInfoTitle="Thông tin tài khoản"
    />
  );
}