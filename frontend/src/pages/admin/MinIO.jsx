import { useEffect, useMemo, useState } from "react";
import * as minioApi from "../../services/minioAdminApi";
import "../../styles/admin/page.css";
import DataTable from "../../components/DataTable";
import CreateFolderModal from "../../components/CreateFolderModal";
import UploadFileModal from "../../components/UploadFileModal";
import InsertMetadataModal from "../../components/InsertMetadataModal";
import FilterModal from "../../components/FilterModal";

function getExt(name = "") {
  const i = name.lastIndexOf(".");
  return i >= 0 ? name.slice(i + 1).toLowerCase() : "";
}

function stripExt(name = "") {
  // bỏ đuôi cuối: a.b.c.pdf -> a.b.c
  return name.replace(/\.[^/.]+$/, "");
}

// LOẠI: document/image/video
function getLoai(name = "") {
  const ext = getExt(name);
  if (["mp4", "mov", "mkv", "avi", "webm"].includes(ext)) return "video";
  if (["png", "jpg", "jpeg", "gif", "webp"].includes(ext)) return "image";
  return "document";
}

// TYPE: pdf/docx/png/mp4...
function getTypeExt(name = "") {
  const ext = getExt(name);
  return ext || "unknown";
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  const kb = bytes / 1024;
  if (kb < 1024) return `${kb.toFixed(1)} KB`;
  const mb = kb / 1024;
  if (mb < 1024) return `${mb.toFixed(1)} MB`;
  const gb = mb / 1024;
  return `${gb.toFixed(2)} GB`;
}

function parentPath(path) {
  if (!path) return "";
  const parts = path.split("/").filter(Boolean);
  parts.pop();
  return parts.join("/");
}

function splitPath(path) {
  return path.split("/").filter(Boolean);
}

function lastName(path) {
  const parts = splitPath(path);
  return parts[parts.length - 1] || "";
}

function normalizeFolderType(x = "") {
  const s = String(x || "").trim().toLowerCase();
  if (s === "topics") return "topic";
  if (s === "lessons") return "lesson";
  if (s === "chunks") return "chunk";
  if (s === "subjects") return "subject";
  return s;
}

export default function MinIO() {
  const [currentPath, setCurrentPath] = useState(""); // "" = root
  const [q, setQ] = useState("");

  const [openCreateFolder, setOpenCreateFolder] = useState(false);
  const [openUpload, setOpenUpload] = useState(false);
  const [openInsert, setOpenInsert] = useState(false);
  const [openFilter, setOpenFilter] = useState(false);

  // filters: loai + type
  const [filters, setFilters] = useState({ loai: "all", type: "all" });

  const [remote, setRemote] = useState({ folders: [], files: [] });
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  // ====== DERIVE ======
  const parts = splitPath(currentPath);
  const section = parts[0] || "";

  const isRoot = currentPath === "";
  const isStorage = ["documents", "images", "video"].includes(section);
  const isMediaStorage = ["images", "video"].includes(section);

  const lastSeg = normalizeFolderType(parts[parts.length - 1] || "");
  const isTypeFolder = ["subject", "topic", "lesson", "chunk"].includes(lastSeg);

  // Hỗ trợ 2 kiểu cấu trúc:
  // A) <bucket>/<class>/<subject>/<category>  (len 4, category = topics/lessons/chunks)
  // B) <bucket>/<class>/<category>            (len 3, category = topics/lessons/chunks)
  const isClassLevel = isStorage && parts.length === 2;
  const isSubjectLevel = isStorage && parts.length === 3 && !isTypeFolder;
  const isCategoryLevel =
    isStorage &&
    ((parts.length === 3 && isTypeFolder) || (parts.length === 4 && isTypeFolder));

  // Với images/video:
  // - nếu backend còn trả folder con => vẫn là folder view
  // - chỉ khi đã đi sâu và không còn folder con nữa => mới là file view
  const hasRemoteFolders = (remote.folders || []).length > 0;
  const isDirectMediaFilePath =
    isMediaStorage &&
    parts.length >= 2 &&
    !hasRemoteFolders;

  const isFileView = isCategoryLevel || isDirectMediaFilePath;
  const isFolderView = isRoot || (isStorage && !isFileView);

  useEffect(() => {
    let alive = true;

    async function load() {
      // root: UI cố định 3 mục
      if (currentPath === "") {
        setRemote({ folders: [], files: [] });
        setErr("");
        setLoading(false);
        return;
      }

      setLoading(true);
      setErr("");

      try {
        const data = await minioApi.minioList(currentPath);
        if (!alive) return;
        setRemote({
          folders: data.folders || [],
          files: data.files || [],
        });
      } catch (e) {
        if (!alive) return;
        setErr(String(e?.message || e));
        setRemote({ folders: [], files: [] });
      } finally {
        alive && setLoading(false);
      }
    }

    load();
    return () => {
      alive = false;
    };
  }, [currentPath]);

  // ====== ROOT rows ======
  const rootRows = useMemo(() => {
    const items = [
      { id: "r-doc", name: "documents", fullPath: "documents", isFixed: true },
      { id: "r-img", name: "images", fullPath: "images", isFixed: true },
      { id: "r-vid", name: "video", fullPath: "video", isFixed: true },
    ];
    const s = q.trim().toLowerCase();
    return !s ? items : items.filter((x) => x.name.toLowerCase().includes(s));
  }, [q]);

  // ====== Child folders (cho cả documents/images/video) ======
  const childFolders = useMemo(() => {
    if (!isStorage || isRoot || isCategoryLevel) return [];

    const s = q.trim().toLowerCase();

    const fixedCats = ["topic", "lesson", "chunk"];

    const rows = (remote.folders || []).map((f) => ({
      id: `f-${f.fullPath}`,
      name: f.name,
      fullPath: f.fullPath,
      isCategory: isSubjectLevel || isClassLevel,
      isFixed:
        (isSubjectLevel || isClassLevel) &&
        fixedCats.includes(normalizeFolderType(f.name || "")),
    }));

    const filtered = !s ? rows : rows.filter((x) => x.name.toLowerCase().includes(s));
    return filtered.sort((a, b) => a.name.localeCompare(b.name));
  }, [remote.folders, q, isStorage, isRoot, isCategoryLevel, isSubjectLevel, isClassLevel]);

  // ====== Files in currentPath ======
  const fileRows = useMemo(() => {
    if (!isFileView) return [];

    const list = (remote.files || []).map((x) => {
      const fullName = x.name || "";
      const baseName = stripExt(fullName);
      const loai = getLoai(fullName);
      const type = getTypeExt(fullName);

      return {
        id: x.object_key,
        name: baseName,
        fullName,
        loai,
        type,
        size: x.size || 0,
        updatedAt: x.last_modified ? x.last_modified.slice(0, 16).replace("T", " ") : "",
        object_key: x.object_key,
        url: x.url,
      };
    });

    const byLoai = filters.loai === "all" ? list : list.filter((r) => r.loai === filters.loai);
    const byType = filters.type === "all" ? byLoai : byLoai.filter((r) => r.type === filters.type);

    const s = q.trim().toLowerCase();
    const searched = !s ? byType : byType.filter((r) => r.name.toLowerCase().includes(s));

    return searched.sort((a, b) => a.name.localeCompare(b.name));
  }, [remote.files, q, filters, isFileView]);

  // danh sách type hiện có để đưa vào dropdown
  const availableTypes = useMemo(() => {
    const set = new Set((remote.files || []).map((f) => getTypeExt(f.name || "")));
    return Array.from(set).filter(Boolean).sort((a, b) => a.localeCompare(b));
  }, [remote.files]);

  // ====== Columns ======
  const folderColumns = [
    {
      key: "name",
      label: "THƯ MỤC",
      render: (r) => (
        <div className="folder-cell">
          <div className="folder-left">
            <div className="folder-icon">📁</div>
            <div className="folder-divider" />
            <div className="folder-name" title={r.name}>
              {r.name}
            </div>
          </div>

          <div className="folder-right">›</div>
        </div>
      ),
    },
  ];

  const fileColumns = [
    {
      key: "name",
      label: "TÊN",
      render: (r) => {
        const icon = r.loai === "video" ? "🎬" : r.loai === "image" ? "🖼️" : "📄";

        return (
          <div className="file-cell">
            <div className="file-left">
              <div className={`file-icon file-${r.loai}`}>{icon}</div>
              <div className="file-divider" />
              <div className="file-name" title={r.fullName}>
                {r.name}
              </div>
            </div>
          </div>
        );
      },
    },

    {
      key: "loai",
      label: "LOẠI",
      render: (r) => {
        const cls = r.loai === "document" ? "other" : r.loai;
        return <span className={`file-type-badge ${cls}`}>{r.loai}</span>;
      },
    },

    {
      key: "type",
      label: "TYPE",
      render: (r) => {
        const cls =
          r.type === "pdf"
            ? "pdf"
            : r.loai === "image"
            ? "image"
            : r.loai === "video"
            ? "video"
            : "other";
        return <span className={`file-type-badge ${cls}`}>{r.type}</span>;
      },
    },

    { key: "size", label: "KÍCH THƯỚC", render: (r) => formatBytes(r.size) },
    { key: "updatedAt", label: "CẬP NHẬT" },
  ];

  // ====== Nav ======
  function goBack() {
    if (isRoot) return;
    setCurrentPath(parentPath(currentPath));
    setQ("");
    setFilters({ loai: "all", type: "all" });
  }

  function openFolder(fullPath) {
    setCurrentPath(fullPath);
    setQ("");
    setFilters({ loai: "all", type: "all" });
  }

  // ====== Create folder rules ======
  function canCreateFolderHere() {
    if (!isStorage) return false;
    if (isCategoryLevel) return false;
    return parts.length === 1 || parts.length === 2 || parts.length === 3;
  }

  async function createFolder(name) {
    const n = name.trim();
    if (!n) return;

    if (!canCreateFolderHere()) {
      alert("Không thể tạo thư mục ở vị trí này.");
      return;
    }
    if (n.includes("/")) {
      alert("Tên folder không được chứa dấu '/'.");
      return;
    }

    const fullPath = currentPath ? `${currentPath}/${n}` : n;

    try {
      await minioApi.createFolder(fullPath);

      if (splitPath(fullPath).length === 3) {
        const defaults = ["topic", "lesson", "chunk"];
        for (const d of defaults) {
          try {
            await minioApi.createFolder(`${fullPath}/${d}`);
          } catch {
            // ignore
          }
        }
      }

      setOpenCreateFolder(false);

      const data = await minioApi.minioList(currentPath);
      setRemote({ folders: data.folders || [], files: data.files || [] });
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  function canEditDeleteFolder(row) {
    if (!row?.fullPath) return false;
    if (!section) return false;

    const inThisBucket = row.fullPath === section || row.fullPath.startsWith(section + "/");
    return inThisBucket;
  }

  async function editFolder(row) {
    const oldPath = row.fullPath;
    const oldName = lastName(oldPath);

    const n = window.prompt("Tên mới:", oldName);
    if (n == null) return;
    const name = n.trim();
    if (!name) return;

    if (name.includes("/")) {
      alert("Tên folder không được chứa dấu '/'.");
      return;
    }

    const p = parentPath(oldPath);
    const newPath = p ? `${p}/${name}` : name;

    try {
      await minioApi.renameFolder(oldPath, newPath);

      setCurrentPath((cp) => {
        if (cp === oldPath) return newPath;
        if (cp.startsWith(oldPath + "/")) return newPath + cp.slice(oldPath.length);
        return cp;
      });

      const parent = parentPath(newPath) || newPath;
      const data = await minioApi.minioList(parent);
      setRemote({ folders: data.folders || [], files: data.files || [] });
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  async function deleteFolderCascade(row) {
    const target = row.fullPath;
    if (!confirm(`Xoá folder "${lastName(target)}" và toàn bộ dữ liệu con?`)) return;

    try {
      await minioApi.deleteFolder(target);

      setCurrentPath((cp) => (cp === target || cp.startsWith(target + "/") ? parentPath(target) : cp));

      const parent = parentPath(target);
      if (parent) {
        const data = await minioApi.minioList(parent);
        setRemote({ folders: data.folders || [], files: data.files || [] });
      } else {
        setRemote({ folders: [], files: [] });
      }
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  // ====== File actions ======
  function canUploadHere() {
    return isStorage && (parts.length === 3 || parts.length === 4);
  }

  function canFilterHere() {
    return isFileView;
  }

  async function uploadFile(file, { onProgress } = {}) {
    if (!canUploadHere()) return;

    try {
      const res = await minioApi.uploadFiles(currentPath, [file], onProgress);
      if (res?.failed_count > 0) {
        const msg = (res.failed || [])
          .map((item) => `${item?.filename || item?.object_key || "file"}: ${item?.error || "Upload chưa sync đủ 4 hệ"}`)
          .join("\n");
        throw new Error(msg || "Upload chưa sync đủ MinIO / MongoDB / PostgreSQL / Neo4j");
      }

      setOpenUpload(false);

      const data = await minioApi.minioList(currentPath);
      setRemote({ folders: data.folders || [], files: data.files || [] });
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  async function insertItem({ meta, file }, { onProgress } = {}) {
    if (!canUploadHere()) return;

    try {
      const res = await minioApi.insertItem(currentPath, meta || {}, file || null, onProgress);
      if (res?.syncStatus && !res.syncStatus.isFullySynced) {
        const parts = res.syncStatus.missing || [];
        throw new Error(parts.length ? parts.join("\n") : "Upload chưa sync đủ MinIO / MongoDB / PostgreSQL / Neo4j");
      }

      setOpenInsert(false);

      const nextPath = res?.path || currentPath;
      if (nextPath !== currentPath) {
        setCurrentPath(nextPath);
      }

      const data = await minioApi.minioList(nextPath);
      setRemote({ folders: data.folders || [], files: data.files || [] });

      if (res?.requested_path && res?.path && res.path !== res.requested_path) {
        alert(`Đã tự chuyển dữ liệu sang đúng lớp: ${res.path}`);
      }
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  async function editFile(row) {
    const oldBase = row.name || "";
    const ext = row.type;

    const input = window.prompt("Đổi tên file:", oldBase);
    if (input == null) return;

    let newBase = input.trim();
    if (!newBase) return;

    if (newBase.includes("/") || newBase.includes("\\")) {
      alert("Tên file không được chứa '/' hoặc '\\'.");
      return;
    }

    const newFullName = ext && ext !== "unknown" ? `${newBase}.${ext}` : newBase;

    try {
      await minioApi.renameObject(row.object_key, newFullName);

      const data = await minioApi.minioList(currentPath);
      setRemote({ folders: data.folders || [], files: data.files || [] });
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  async function deleteFile(row) {
    if (!confirm(`Xoá "${row.fullName || row.name}"?`)) return;

    try {
      await minioApi.deleteObject(row.object_key);
      const data = await minioApi.minioList(currentPath);
      setRemote({ folders: data.folders || [], files: data.files || [] });
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  const headerTitle = useMemo(() => {
    if (isRoot) return "MinIO";
    if (currentPath === "documents") return "Documents";
    if (currentPath === "images") return "Images";
    if (currentPath === "video") return "Video";
    return lastName(currentPath);
  }, [isRoot, currentPath]);

  const hasFolderData = isRoot ? rootRows.length > 0 : childFolders.length > 0;
  const hasFileData = fileRows.length > 0;

  return (
    <div className="minio-page">
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
              {splitPath(currentPath).map((p, idx, arr) => (
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
              placeholder={isFileView ? "Tìm file..." : "Tìm folder..."}
              value={q}
              onChange={(e) => setQ(e.target.value)}
            />
          </div>

          <div className="header-actions">
            {canCreateFolderHere() && (
              <button className="btn btn-primary" onClick={() => setOpenCreateFolder(true)}>
                + Folder
              </button>
            )}

            {canUploadHere() && (
              <>
                {/* <button className="btn btn-primary" onClick={() => setOpenUpload(true)}>
                  Upload
                </button> */}
                <button className="btn btn-primary" onClick={() => setOpenInsert(true)}>
                  Upload
                </button>
              </>
            )}

            {canFilterHere() && (
              <button className="btn" onClick={() => setOpenFilter(true)}>
                Filter
              </button>
            )}
          </div>
        </div>
      </div>

      <div className="table-wrapper minio-table-wrapper">
        {loading ? (
          <div className="empty-state">
            <p>Đang tải...</p>
          </div>
        ) : err ? (
          <div className="empty-state">
            <p style={{ color: "crimson" }}>{err}</p>
          </div>
        ) : isFolderView ? (
          hasFolderData ? (
            <DataTable
              columns={folderColumns}
              rows={isRoot ? rootRows : childFolders}
              pageSize={-1}
              showPagination={false}
              getRowClassName={() => "row-click"}
              onRowDoubleClick={(row) => openFolder(row.fullPath)}
              renderActions={
                isRoot
                  ? null
                  : (row) => {
                      if (!canEditDeleteFolder(row)) return null;
                      return (
                        <div className="table-actions" onDoubleClick={(e) => e.stopPropagation()}>
                          <button
                            className="btn"
                            onClick={(e) => {
                              e.stopPropagation();
                              editFolder(row);
                            }}
                            onDoubleClick={(e) => e.stopPropagation()}
                          >
                            Sửa
                          </button>
                          <button
                            className="btn"
                            onClick={(e) => {
                              e.stopPropagation();
                              deleteFolderCascade(row);
                            }}
                            onDoubleClick={(e) => e.stopPropagation()}
                          >
                            Xoá
                          </button>
                        </div>
                      );
                    }
              }
            />
          ) : (
            <div className="empty-state">
              <div className="empty-state-icon">📂</div>
              <p>{isRoot ? "Không có dữ liệu." : "Không tìm thấy thư mục nào."}</p>
            </div>
          )
        ) : hasFileData ? (
          <DataTable
            columns={fileColumns}
            rows={fileRows}
            pageSize={-1}
            showPagination={false}
            renderActions={(row) => (
              <div className="table-actions">
                <button className="btn" onClick={() => editFile(row)}>
                  Sửa
                </button>
                <button className="btn" onClick={() => deleteFile(row)}>
                  Xoá
                </button>
              </div>
            )}
          />
        ) : (
          <div className="empty-state">
            <div className="empty-state-icon">📄</div>
            <p>{q ? `Không tìm thấy file với "${q}"` : "Thư mục trống."}</p>
          </div>
        )}
      </div>

      <CreateFolderModal
        open={openCreateFolder}
        onClose={() => setOpenCreateFolder(false)}
        onCreate={createFolder}
      />

      <UploadFileModal
        open={openUpload}
        onClose={() => setOpenUpload(false)}
        folderName={currentPath}
        onUpload={uploadFile}
      />

      <InsertMetadataModal
        open={openInsert}
        onClose={() => setOpenInsert(false)}
        folderName={currentPath}
        onInsert={insertItem}
      />

      <FilterModal
        open={openFilter}
        onClose={() => setOpenFilter(false)}
        initialValue={filters}
        availableTypes={availableTypes}
        onApply={(v) => setFilters(v)}
      />
    </div>
  );
}