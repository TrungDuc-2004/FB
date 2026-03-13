import { Navigate, useParams, useSearchParams } from "react-router-dom";

export default function DocumentView() {
  const { chunkID } = useParams();
  const [searchParams] = useSearchParams();
  const currentType = (searchParams.get("type") || "document").trim() || "document";

  return <Navigate to={`/user/docs/${encodeURIComponent(chunkID)}?type=${encodeURIComponent(currentType)}`} replace />;
}
