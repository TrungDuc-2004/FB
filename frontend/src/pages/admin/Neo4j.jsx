// pages/admin/Neo4j.jsx
import { useEffect, useMemo, useState } from "react";
import "../../styles/admin/page.css";
import DataTable from "../../components/DataTable";
import * as neoApi from "../../services/neoAdminApi";

export default function Neo4j() {
  const [currentLabel, setCurrentLabel] = useState("");
  const [currentNodeId, setCurrentNodeId] = useState("");
  const [q, setQ] = useState("");

  const [labels, setLabels] = useState([]); // {id,name,count}
  const [nodes, setNodes] = useState([]); // {id,postgreId,name,updatedAt,props}
  const [selectedNode, setSelectedNode] = useState(null);

  const isRoot = currentLabel === "";
  const isNodeDetail = !!currentNodeId;

  async function reloadLabels() {
    const data = await neoApi.listLabels();
    setLabels(data.labels || []);
  }

  async function reloadNodes(label) {
    const data = await neoApi.listNodes(label);
    setNodes(data.nodes || []);
  }

  async function reloadNodeDetail(nodeId) {
    const data = await neoApi.getNode(nodeId);
    setSelectedNode(data.node || null);
  }

  useEffect(() => {
    reloadLabels().catch((e) => console.error(e));
  }, []);

  useEffect(() => {
    if (!currentLabel) return;
    reloadNodes(currentLabel)
      .then(() => setSelectedNode(null))
      .catch((e) => console.error(e));
  }, [currentLabel]);

  useEffect(() => {
    if (!currentNodeId) {
      setSelectedNode(null);
      return;
    }
    reloadNodeDetail(currentNodeId).catch((e) => console.error(e));
  }, [currentNodeId]);

  const headerTitle = useMemo(() => {
    if (isRoot) return "Neo4j";
    if (isNodeDetail) return selectedNode?.name || currentLabel;
    return currentLabel;
  }, [isRoot, isNodeDetail, selectedNode, currentLabel]);

  const breadcrumbParts = useMemo(() => {
    if (isRoot) return [];
    const parts = ["neo4j", currentLabel];
    if (isNodeDetail) parts.push(selectedNode?.name || currentNodeId);
    return parts;
  }, [isRoot, currentLabel, isNodeDetail, selectedNode, currentNodeId]);

  function goBack() {
    if (isNodeDetail) {
      setCurrentNodeId("");
      setQ("");
      return;
    }
    if (!isRoot) {
      setCurrentLabel("");
      setNodes([]);
      setQ("");
      return;
    }
  }

  function openLabel(row) {
    setCurrentLabel(row.name);
    setCurrentNodeId("");
    setQ("");
  }

  function openNode(row) {
    setCurrentNodeId(row.id);
    setQ("");
  }

  const labelRows = useMemo(() => {
    const s = q.trim().toLowerCase();
    const list = !s ? labels : labels.filter((l) => (l.name || "").toLowerCase().includes(s));
    return list.slice().sort((a, b) => (a.name || "").localeCompare(b.name || ""));
  }, [labels, q]);

  const nodeRows = useMemo(() => {
    const s = q.trim().toLowerCase();
    const list = !s
      ? nodes
      : nodes.filter(
          (n) =>
            String(n.postgreId ?? "")
              .toLowerCase()
              .includes(s) ||
            String(n.name || "")
              .toLowerCase()
              .includes(s)
        );

    return list
      .slice()
      .sort((a, b) => String(b.updatedAt || "").localeCompare(String(a.updatedAt || "")));
  }, [nodes, q]);

  const detailRows = useMemo(() => {
    if (!selectedNode) return [];

    const idKey = selectedNode.entity_id_key || "id";
    const nameKey = selectedNode.entity_name_key || "name";

    return [
      { id: "entity_id", k: idKey, v: String(selectedNode.entity_id || "") },
      { id: "entity_name", k: nameKey, v: String(selectedNode.entity_name || "") },
      { id: "relation", k: "relation", v: String(selectedNode.relation || "") },
    ];
  }, [selectedNode]);

  const labelColumns = [
    {
      key: "name",
      label: "NODE TYPE",
      render: (r) => (
        <div className="folder-cell">
          <div className="folder-left">
            <div className="folder-icon">⬢</div>
            <div className="folder-divider" />
            <div className="folder-name" title={r.name}>
              {r.name}
            </div>
          </div>
          <div className="folder-right">›</div>
        </div>
      ),
    },
    { key: "count", label: "COUNT", render: (r) => <span className="crumb">{r.count ?? ""}</span> },
  ];

  const nodeColumns = [
    {
      key: "postgreId",
      label: "POSTGREID",
      render: (r) => <span className="crumb">{String(r.postgreId ?? "")}</span>,
    },
    {
      key: "name",
      label: "NAME",
      render: (r) => (
        <div className="file-cell">
          <div className="file-left">
            <div className="file-icon file-other">◉</div>
            <div className="file-divider" />
            <div className="file-name" title={r.name || ""}>
              {r.name || "(no name)"}
            </div>
          </div>
        </div>
      ),
    },
  ];

  const detailColumns = [
    { key: "k", label: "FIELD", render: (r) => <span className="crumb">{r.k}</span> },
    {
      key: "v",
      label: "VALUE",
      render: (r) => (
        <span
          title={r.v}
          style={{
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
            display: "block",
            maxWidth: 560,
          }}
        >
          {r.v}
        </span>
      ),
    },
  ];

  return (
    <div>
      <div className="page-header">
        <div className="page-header-top">
          <div className="title-row">
            <h2 className="page-title">{headerTitle}</h2>
            {!isRoot && (
              <button className="back-btn back-btn-right" onClick={goBack}>
                Back →
              </button>
            )}
          </div>

          {!isRoot && (
            <div className="breadcrumb">
              {breadcrumbParts.map((p, idx, arr) => (
                <span key={idx} className="crumb">
                  {p}
                  {idx < arr.length - 1 ? <span className="sep">/</span> : null}
                </span>
              ))}
            </div>
          )}
        </div>

        <div className="page-header-bottom">
          <div className="search-box">
            <input
              placeholder={
                isRoot
                  ? "Tìm node type..."
                  : isNodeDetail
                    ? "Đang xem chi tiết..."
                    : "Tìm node (name hoặc postgreId)..."
              }
              value={q}
              onChange={(e) => setQ(e.target.value)}
              disabled={isNodeDetail}
            />
          </div>

          <div className="header-actions">
            <span className="crumb" style={{ opacity: 0.7 }}>
              View only
            </span>
          </div>
        </div>
      </div>

      <div className="table-wrapper">
        {isRoot ? (
          <DataTable
            pageSize={7}
            columns={labelColumns}
            rows={labelRows}
            getRowClassName={() => "row-click"}
            onRowDoubleClick={(row) => openLabel(row)}
            renderActions={null}
          />
        ) : isNodeDetail ? (
          <DataTable columns={detailColumns} rows={detailRows} renderActions={null} />
        ) : (
          <DataTable
            pageSize={7}
            columns={nodeColumns}
            rows={nodeRows}
            getRowClassName={() => "row-click"}
            onRowDoubleClick={(row) => openNode(row)}
            renderActions={null}
          />
        )}
      </div>
    </div>
  );
}
