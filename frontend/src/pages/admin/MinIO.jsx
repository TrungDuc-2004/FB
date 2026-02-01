import { useEffect, useMemo, useState } from "react";
import * as minioApi from "../../services/minioAdminApi";
import "../../styles/admin/page.css";
import DataTable from "../../components/DataTable";
import CreateFolderModal from "../../components/CreateFolderModal";
import UploadFileModal from "../../components/UploadFileModal";
import InsertMetadataModal from "../../components/InsertMetadataModal";
import FilterModal from "../../components/FilterModal";

function nowStr() {
  return new Date().toISOString().slice(0, 16).replace("T", " ");
}

function getExt(name = "") {
  const i = name.lastIndexOf(".");
  return i >= 0 ? name.slice(i + 1).toLowerCase() : "";
}

function getFileType(name = "") {
  const ext = getExt(name);
  if (ext === "pdf") return "pdf";
  if (["mp4", "mov", "mkv", "avi", "webm"].includes(ext)) return "video";
  if (["png", "jpg", "jpeg", "gif", "webp"].includes(ext)) return "image";
  return "other";
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

function makeDefaultCats() {
  const t = Date.now();
  return [
    { id: `cat-${t}-topic`, name: "topic" },
    { id: `cat-${t}-lesson`, name: "lesson" },
    { id: `cat-${t}-chunk`, name: "chunk" },
  ];
}

export default function MinIO() {
  const [currentPath, setCurrentPath] = useState(""); // "" = root
  const [q, setQ] = useState("");

  const [openCreateFolder, setOpenCreateFolder] = useState(false);
  const [openUpload, setOpenUpload] = useState(false);
  const [openInsert, setOpenInsert] = useState(false);
  const [openFilter, setOpenFilter] = useState(false);
  const [filters, setFilters] = useState({ type: "all" });
  const [remote, setRemote] = useState({ folders: [], files: [] });
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  // ====== DERIVE ======
  const parts = splitPath(currentPath);
  const section = parts[0] || "";

  const isRoot = currentPath === "";
  const isImages = currentPath === "images";
  const isVideo = currentPath === "video";
  const isDocuments = section === "documents";

  const isDocsSubject = isDocuments && parts.length === 3; // documents/class-10/tin-hoc
  const isDocsCategory = isDocuments && parts.length === 4; // bất kỳ folder level 4 => file view

  const isFileView = isImages || isVideo || isDocsCategory;
  const isFolderView = isRoot || (isDocuments && !isDocsCategory);

  useEffect(() => {
    let alive = true;

    async function load() {
      // root không cần gọi API vì UI bạn cố định 3 mục
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

  // ====== Child folders for documents ======
  const docChildFolders = useMemo(() => {
    if (!isDocuments) return [];

    const s = q.trim().toLowerCase();

    const rows = (remote.folders || []).map((f) => ({
      id: `f-${f.fullPath}`,
      name: f.name,
      fullPath: f.fullPath,
      // nếu đang ở subject (level 3) thì folder con là category
      isCategory: isDocsSubject,
      subjectPath: isDocsSubject ? currentPath : undefined,
    }));

    const filtered = !s ? rows : rows.filter((x) => x.name.toLowerCase().includes(s));
    return filtered.sort((a, b) => a.name.localeCompare(b.name));
  }, [remote.folders, q, isDocuments, isDocsSubject, currentPath]);

  // ====== Files in currentPath ======
  const fileRows = useMemo(() => {
    if (!isFileView) return [];

    const list = (remote.files || []).map((x) => ({
      id: x.object_key,
      name: x.name,
      size: x.size || 0,
      updatedAt: x.last_modified ? x.last_modified.slice(0, 16).replace("T", " ") : "",
      meta: { object_key: x.object_key, url: x.url },
      object_key: x.object_key,
      url: x.url,
    }));

    const byType =
      filters.type === "all" ? list : list.filter((r) => getFileType(r.name) === filters.type);

    const s = q.trim().toLowerCase();
    const searched = !s ? byType : byType.filter((r) => r.name.toLowerCase().includes(s));

    return searched.sort((a, b) => a.name.localeCompare(b.name));
  }, [remote.files, currentPath, q, filters, isFileView]);

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
      label: "TÊN FILE",
      render: (r) => {
        const type = getFileType(r.name);

        const icon =
          type === "pdf" ? "📄" : type === "video" ? "🎬" : type === "image" ? "🖼️" : "📦";

        return (
          <div className="file-cell">
            <div className="file-left">
              <div className={`file-icon file-${type}`}>{icon}</div>
              <div className="file-divider" />
              <div className="file-name" title={r.name}>
                {r.name}
              </div>
            </div>
          </div>
        );
      },
    },

    {
      key: "type",
      label: "LOẠI",
      render: (r) => {
        const type = getFileType(r.name);
        return <span className={`file-type-badge ${type}`}>{type}</span>;
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
    setFilters({ type: "all" });
  }

  function openFolder(fullPath) {
    setCurrentPath(fullPath);
    setQ("");
    setFilters({ type: "all" });
  }

  // ====== Create folder rules ======
  function canCreateFolderHere() {
    if (!isDocuments) return false;
    if (isDocsCategory) return false;
    // cho tạo ở: documents (1), class (2), subject (3)
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

      // nếu vừa tạo SUBJECT (level 3) => auto tạo topic/lesson/chunk giống UI cũ
      if (splitPath(fullPath).length === 3) {
        const defaults = ["topic", "lesson", "chunk"];
        for (const d of defaults) {
          try {
            await minioApi.createFolder(`${fullPath}/${d}`);
          } catch {
            // ignore nếu đã tồn tại
          }
        }
      }

      setOpenCreateFolder(false);

      // reload lại danh sách folder hiện tại
      const data = await minioApi.minioList(currentPath);
      setRemote({ folders: data.folders || [], files: data.files || [] });
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  // ====== Edit/Delete folder ======
  function canEditDeleteFolder(row) {
    if (row?.isFixed) return false;
    if (row?.isCategory) return true; // subject cats
    const len = splitPath(row.fullPath).length;
    return row.fullPath.startsWith("documents/") && (len === 2 || len === 3);
  }

  function renameFolderPath(oldPath, newPath) {
    // update folders
    setFolders((prev) =>
      prev.map((f) => {
        if (f.path === oldPath || f.path.startsWith(oldPath + "/")) {
          return { ...f, path: newPath + f.path.slice(oldPath.length) };
        }
        return f;
      })
    );

    // update filesByFolder keys
    setFilesByFolder((prev) => {
      const next = {};
      for (const [k, v] of Object.entries(prev)) {
        if (k === oldPath || k.startsWith(oldPath + "/")) {
          const nk = newPath + k.slice(oldPath.length);
          next[nk] = v;
        } else {
          next[k] = v;
        }
      }
      return next;
    });

    // update subjectCats keys
    setSubjectCats((prev) => {
      const next = {};
      for (const [k, v] of Object.entries(prev)) {
        if (k === oldPath || k.startsWith(oldPath + "/")) {
          const nk = newPath + k.slice(oldPath.length);
          next[nk] = v;
        } else {
          next[k] = v;
        }
      }
      return next;
    });

    // update currentPath if inside
    setCurrentPath((cp) => {
      if (cp === oldPath || cp.startsWith(oldPath + "/")) {
        return newPath + cp.slice(oldPath.length);
      }
      return cp;
    });

    setQ("");
    setFilters({ type: "all" });
  }

  function editCategory(row) {
    const subjectPath = row.subjectPath;
    const oldName = row.name;
    const oldFull = row.fullPath;

    const n = window.prompt("Tên mới:", oldName);
    if (n == null) return;
    const name = n.trim();
    if (!name) return;

    if (name.includes("/")) {
      alert("Tên folder không được chứa dấu '/'.");
      return;
    }

    const cats = subjectCats[subjectPath] || [];
    if (cats.some((c) => c.name === name)) {
      alert("Tên folder bị trùng trong subject này!");
      return;
    }

    const newFull = `${subjectPath}/${name}`;

    setSubjectCats((prev) => ({
      ...prev,
      [subjectPath]: (prev[subjectPath] || []).map((c) => (c.id === row.id ? { ...c, name } : c)),
    }));

    setFilesByFolder((prev) => {
      const next = { ...prev };
      if (oldFull in next) {
        next[newFull] = next[oldFull];
        delete next[oldFull];
      } else {
        next[newFull] = next[newFull] || [];
      }
      return next;
    });

    setCurrentPath((cp) => (cp === oldFull ? newFull : cp));
    setQ("");
    setFilters({ type: "all" });
  }

  function deleteCategory(row) {
    const subjectPath = row.subjectPath;
    const full = row.fullPath;

    if (!confirm(`Xoá folder "${row.name}" và toàn bộ file bên trong? (demo)`)) return;

    setSubjectCats((prev) => ({
      ...prev,
      [subjectPath]: (prev[subjectPath] || []).filter((c) => c.id !== row.id),
    }));

    setFilesByFolder((prev) => {
      const next = { ...prev };
      delete next[full];
      return next;
    });

    setCurrentPath((cp) => (cp === full ? subjectPath : cp));
    setQ("");
    setFilters({ type: "all" });
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

      // nếu đang đứng trong folder bị rename => cập nhật currentPath
      setCurrentPath((cp) => {
        if (cp === oldPath) return newPath;
        if (cp.startsWith(oldPath + "/")) return newPath + cp.slice(oldPath.length);
        return cp;
      });

      // reload (theo currentPath mới)
      const data = await minioApi.minioList(parentPath(newPath) || newPath);
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

      // nếu currentPath đang nằm trong folder bị xoá => bật lên cha
      setCurrentPath((cp) =>
        cp === target || cp.startsWith(target + "/") ? parentPath(target) : cp
      );

      // reload folder cha
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
  function canFileActionsHere() {
    return isFileView;
  }

  async function uploadFile(file) {
    if (!canFileActionsHere()) return;

    try {
      await minioApi.uploadFiles(currentPath, [file]);
      setOpenUpload(false);

      const data = await minioApi.minioList(currentPath);
      setRemote({ folders: data.folders || [], files: data.files || [] });
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  async function insertItem({ meta, file }) {
    if (!canFileActionsHere()) return;

    try {
      await minioApi.insertItem(currentPath, meta || {}, file || null);
      setOpenInsert(false);

      const data = await minioApi.minioList(currentPath);
      setRemote({ folders: data.folders || [], files: data.files || [] });
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  async function editFile(row) {
    const oldName = row.name || "";
    const input = window.prompt("Đổi tên file:", oldName);
    if (input == null) return; // bấm Cancel

    let newName = input.trim();
    if (!newName) return;

    // chặn ký tự path
    if (newName.includes("/") || newName.includes("\\")) {
      alert("Tên file không được chứa '/' hoặc '\\'.");
      return;
    }

    // (Tuỳ chọn) nếu user nhập không có đuôi, tự giữ đuôi cũ
    const oldExt = oldName.includes(".") ? oldName.split(".").pop() : "";
    const hasExt = newName.includes(".");
    if (oldExt && !hasExt) newName = `${newName}.${oldExt}`;

    try {
      await minioApi.renameObject(row.object_key, newName);

      // reload lại list để UI cập nhật
      const data = await minioApi.minioList(currentPath);
      setRemote({ folders: data.folders || [], files: data.files || [] });
    } catch (e) {
      alert(String(e?.message || e));
    }
  }

  async function deleteFile(row) {
    if (!confirm(`Xoá "${row.name}"?`)) return;

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
    if (isImages) return "Images";
    if (isVideo) return "Video";
    return lastName(currentPath);
  }, [isRoot, isImages, isVideo, currentPath]);

  const hasFolderData = isRoot ? rootRows.length > 0 : docChildFolders.length > 0;
  const hasFileData = fileRows.length > 0;

  return (
    <div>
      {/* HEADER (gọn) */}
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
              placeholder={canFileActionsHere() ? "Tìm file..." : "Tìm folder..."}
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

            {canFileActionsHere() && (
              <>
                <button className="btn btn-primary" onClick={() => setOpenUpload(true)}>
                  Upload
                </button>
                <button className="btn btn-primary" onClick={() => setOpenInsert(true)}>
                  Insert
                </button>
                <button className="btn" onClick={() => setOpenFilter(true)}>
                  Filter
                </button>
              </>
            )}
          </div>
        </div>
      </div>

      {/* CONTENT */}
      <div className="table-wrapper">
        {isFolderView ? (
          hasFolderData ? (
            <DataTable
              columns={folderColumns}
              rows={isRoot ? rootRows : docChildFolders}
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

      {/* MODALS */}
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
        onApply={(v) => setFilters(v)}
      />
    </div>
  );
}
