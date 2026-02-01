import DashboardShell, { DashboardIcons } from "./DashboardShell";

export default function UserLayout() {
  const { IHome, IMinio, IDatabase, INeo4j, IUser } = DashboardIcons;

  // UI giống Admin, nhưng route dưới /user
  const navItems = [
    { to: "/user", label: "Trang chủ", icon: IHome, end: true },
    { to: "/user/minio", label: "MinIO", icon: IMinio },
    { to: "/user/mongo", label: "MongoDB", icon: IDatabase },
    { to: "/user/postgres", label: "PostgreSQL", icon: IDatabase },
    { to: "/user/neo4j", label: "Neo4j", icon: INeo4j },
    { to: "/user/profile", label: "User", icon: IUser },
  ];

  return <DashboardShell basePath="/user" navItems={navItems} />;
}
