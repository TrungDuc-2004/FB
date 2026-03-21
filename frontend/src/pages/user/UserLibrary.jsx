import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import "../../styles/admin/page.css";
import "../../styles/user/library.css";
import DocumentCard from "../../components/DocumentCard";
import {
  listClasses,
  listLessons,
  listChunks,
  listSubjects,
  listTopics,
  toggleSave,
} from "../../services/userDocsApi";

function safeText(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) return text;
  }
  return "";
}

function normalizeText(value = "") {
  return String(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function matchesKeyword(target = "", keyword = "") {
  const source = normalizeText(target);
  const query = normalizeText(keyword);
  if (!query) return true;
  return source.includes(query);
}

function buildMediaItems(chunks = [], kind = "image") {
  const list = [];

  for (const doc of chunks || []) {
    const bucket = kind === "video" ? doc?.videos : doc?.images;
    for (const item of Array.isArray(bucket) ? bucket : []) {
      const itemId = safeText(item?.id, item?.url, item?.name);
      if (!itemId) continue;
      list.push({
        chunkID: itemId,
        chunkName: safeText(item?.name, itemId),
        chunkDescription: safeText(item?.description, doc?.chunkDescription),
        chunkUrl: safeText(item?.url),
        itemType: kind,
        category: kind,
        isSaved: !!item?.isSaved,
        class: doc?.class || { classID: "", className: "" },
        subject: doc?.subject || { subjectID: "", subjectName: "" },
        topic: doc?.topic || { topicID: "", topicName: "" },
        lesson: doc?.lesson || { lessonID: "", lessonName: "" },
        sourceChunkID: doc?.chunkID || "",
      });
    }
  }

  return list;
}

function countMedia(chunks = [], kind = "image") {
  return buildMediaItems(chunks, kind).length;
}

function LibraryTreeButton({ active, label, meta, onClick }) {
  return (
    <button
      type="button"
      className={`library-tree-button${active ? " active" : ""}`}
      onClick={onClick}
    >
      <strong>{label}</strong>
      {meta ? <span>{meta}</span> : null}
    </button>
  );
}

function BrowseTile({ title, subtitle, badge, onOpen }) {
  return (
    <button type="button" className="library-browse-tile" onClick={onOpen}>
      {badge ? <div className="library-browse-badge">{badge}</div> : null}
      <strong>{title}</strong>
      {subtitle ? <span>{subtitle}</span> : null}
    </button>
  );
}

function TabButton({ active, children, onClick, count }) {
  return (
    <button
      type="button"
      className={`library-tab-button${active ? " active" : ""}`}
      onClick={onClick}
    >
      <span>{children}</span>
      {typeof count === "number" ? <strong>{count}</strong> : null}
    </button>
  );
}

function EmptyState({ title, text }) {
  return (
    <div className="library-empty-state">
      <strong>{title}</strong>
      <span>{text}</span>
    </div>
  );
}

export default function UserLibrary() {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [classes, setClasses] = useState([]);
  const [subjects, setSubjects] = useState([]);
  const [topics, setTopics] = useState([]);
  const [lessons, setLessons] = useState([]);
  const [chunks, setChunks] = useState([]);

  const [classID, setClassID] = useState("");
  const [subjectID, setSubjectID] = useState("");
  const [topicID, setTopicID] = useState("");
  const [lessonID, setLessonID] = useState("");
  const [sort, setSort] = useState("name");
  const [localQuery, setLocalQuery] = useState("");
  const [contentTab, setContentTab] = useState("all");

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError("");
        const res = await listClasses();
        setClasses(Array.isArray(res?.items) ? res.items : []);
      } catch (e) {
        setError(String(e?.message || e));
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  const selectedClass = useMemo(
    () => classes.find((item) => item.classID === classID) || null,
    [classes, classID]
  );
  const selectedSubject = useMemo(
    () => subjects.find((item) => item.subjectID === subjectID) || null,
    [subjects, subjectID]
  );
  const selectedTopic = useMemo(
    () => topics.find((item) => item.topicID === topicID) || null,
    [topics, topicID]
  );
  const selectedLesson = useMemo(
    () => lessons.find((item) => item.lessonID === lessonID) || null,
    [lessons, lessonID]
  );

  const breadcrumb = useMemo(
    () =>
      [
        selectedClass?.className || classID,
        selectedSubject?.subjectName || subjectID,
        selectedTopic?.topicName || topicID,
        selectedLesson?.lessonName || lessonID,
      ].filter(Boolean),
    [
      classID,
      lessonID,
      selectedClass?.className,
      selectedLesson?.lessonName,
      selectedSubject?.subjectName,
      selectedTopic?.topicName,
      subjectID,
      topicID,
    ]
  );

  async function fetchSubjects(nextClassID) {
    const res = await listSubjects({ classID: nextClassID });
    const items = Array.isArray(res?.items) ? res.items : [];
    setSubjects(items);
    return items;
  }

  async function fetchTopics(nextSubjectID) {
    const res = await listTopics({ subjectID: nextSubjectID });
    const items = Array.isArray(res?.items) ? res.items : [];
    setTopics(items);
    return items;
  }

  async function fetchLessons(nextTopicID) {
    const res = await listLessons({ topicID: nextTopicID });
    const items = Array.isArray(res?.items) ? res.items : [];
    setLessons(items);
    return items;
  }

  async function fetchChunks(nextLessonID, nextSort = sort) {
    const res = await listChunks({ lessonID: nextLessonID, sort: nextSort });
    const items = Array.isArray(res?.items) ? res.items : [];
    setChunks(items);
    return items;
  }

  async function handleSelectClass(nextClassID) {
    setClassID(nextClassID);
    setSubjectID("");
    setTopicID("");
    setLessonID("");
    setSubjects([]);
    setTopics([]);
    setLessons([]);
    setChunks([]);
    setLocalQuery("");
    setContentTab("all");

    if (!nextClassID) return;

    try {
      setLoading(true);
      setError("");
      await fetchSubjects(nextClassID);
    } catch (e) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function handleSelectSubject(nextSubjectID) {
    setSubjectID(nextSubjectID);
    setTopicID("");
    setLessonID("");
    setTopics([]);
    setLessons([]);
    setChunks([]);
    setLocalQuery("");
    setContentTab("all");

    if (!nextSubjectID) return;

    try {
      setLoading(true);
      setError("");
      await fetchTopics(nextSubjectID);
    } catch (e) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function handleSelectTopic(nextTopicID) {
    setTopicID(nextTopicID);
    setLessonID("");
    setLessons([]);
    setChunks([]);
    setLocalQuery("");
    setContentTab("all");

    if (!nextTopicID) return;

    try {
      setLoading(true);
      setError("");
      await fetchLessons(nextTopicID);
    } catch (e) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function handleSelectLesson(nextLessonID, nextSort = sort) {
    setLessonID(nextLessonID);
    setChunks([]);
    setLocalQuery("");
    setContentTab("all");

    if (!nextLessonID) return;

    try {
      setLoading(true);
      setError("");
      await fetchChunks(nextLessonID, nextSort);
    } catch (e) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function handleChangeSort(nextSort) {
    setSort(nextSort);
    if (!lessonID) return;

    try {
      setLoading(true);
      setError("");
      await fetchChunks(lessonID, nextSort);
    } catch (e) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function handleToggleSave(doc) {
    try {
      const category = doc?.category || doc?.itemType || "document";
      const res = await toggleSave(doc.chunkID, category);

      if (category === "document" || category === "chunk") {
        setChunks((prev) =>
          prev.map((item) =>
            item.chunkID === doc.chunkID ? { ...item, isSaved: !!res.saved } : item
          )
        );
        return;
      }

      setChunks((prev) =>
        prev.map((item) => {
          const bucketName = category === "video" ? "videos" : "images";
          const nextBucket = (Array.isArray(item?.[bucketName]) ? item[bucketName] : []).map((media) => {
            const mediaId = safeText(media?.id, media?.url, media?.name);
            if (mediaId !== doc.chunkID) return media;
            return { ...media, isSaved: !!res.saved };
          });
          return { ...item, [bucketName]: nextBucket };
        })
      );
    } catch (e) {
      setError(String(e?.message || e));
    }
  }

  const filteredSubjects = useMemo(() => {
    return (subjects || []).filter((item) => {
      if (!localQuery.trim()) return true;
      return matchesKeyword(item?.subjectName || item?.subjectID, localQuery);
    });
  }, [subjects, localQuery]);

  const filteredTopics = useMemo(() => {
    return (topics || []).filter((item) => {
      if (!localQuery.trim()) return true;
      return matchesKeyword(item?.topicName || item?.topicID, localQuery);
    });
  }, [topics, localQuery]);

  const filteredLessons = useMemo(() => {
    return (lessons || []).filter((item) => {
      if (!localQuery.trim()) return true;
      return matchesKeyword(item?.lessonName || item?.lessonID, localQuery);
    });
  }, [lessons, localQuery]);

  const filteredChunks = useMemo(() => {
    return (chunks || []).filter((item) => {
      if (!localQuery.trim()) return true;
      const haystack = [
        item?.chunkName,
        item?.chunkDescription,
        ...(Array.isArray(item?.keywords) ? item.keywords : []),
      ].join(" ");
      return matchesKeyword(haystack, localQuery);
    });
  }, [chunks, localQuery]);

  const imageItems = useMemo(() => buildMediaItems(filteredChunks, "image"), [filteredChunks]);
  const videoItems = useMemo(() => buildMediaItems(filteredChunks, "video"), [filteredChunks]);

  const viewMode = lessonID
    ? "content"
    : topicID
    ? "lessons"
    : subjectID
    ? "topics"
    : classID
    ? "subjects"
    : "classes";

  const statCards = useMemo(
    () => [
      { label: "Sách", value: subjects.length },
      { label: "Chủ đề", value: topics.length },
      { label: "Bài học", value: lessons.length },
      { label: "Tài liệu", value: chunks.length },
      { label: "Ảnh", value: countMedia(chunks, "image") },
      { label: "Video", value: countMedia(chunks, "video") },
    ],
    [chunks, lessons.length, subjects.length, topics.length]
  );

  const tabCounts = {
    all: filteredChunks.length + imageItems.length + videoItems.length,
    document: filteredChunks.length,
    image: imageItems.length,
    video: videoItems.length,
  };

  return (
    <div className="library-page-shell">
      <section className="library-hero">
        <div>
          <div className="library-hero-eyebrow">THƯ VIỆN HỌC LIỆU</div>
          <h1>Duyệt theo lớp, sách, chủ đề và bài học để mở đúng tài liệu.</h1>
          <p>
            Thư viện này tách rõ từng bước duyệt nội dung: chọn lớp, chọn sách, mở chủ đề, mở bài học rồi
            mới xem tài liệu, hình ảnh và video liên quan.
          </p>
          <div className="library-breadcrumb-row">
            <span>Đường dẫn</span>
            <strong>{breadcrumb.length ? breadcrumb.join(" / ") : "Chưa chọn nhánh"}</strong>
          </div>
        </div>

        <div className="library-stat-grid">
          {statCards.map((item) => (
            <div key={item.label} className="library-stat-card">
              <span>{item.label}</span>
              <strong>{item.value}</strong>
            </div>
          ))}
        </div>
      </section>

      <div className="library-main-grid">
        <aside className="library-sidebar">
          <div className="library-sidebar-section">
            <div className="library-sidebar-title">Lớp học</div>
            <div className="library-tree-list">
              {classes.map((item) => (
                <LibraryTreeButton
                  key={item.classID}
                  active={classID === item.classID}
                  label={item.className || item.classID}
                  meta={item.classID}
                  onClick={() => handleSelectClass(item.classID)}
                />
              ))}
            </div>
          </div>

          <div className="library-sidebar-section">
            <div className="library-sidebar-title">Sách</div>
            <div className="library-tree-list compact">
              {subjects.length ? (
                subjects.map((item) => (
                  <LibraryTreeButton
                    key={item.subjectID}
                    active={subjectID === item.subjectID}
                    label={item.subjectName || item.subjectID}
                    meta={item.subjectID}
                    onClick={() => handleSelectSubject(item.subjectID)}
                  />
                ))
              ) : (
                <EmptyState title="Chưa có sách" text="Chọn lớp để tải danh sách sách/môn học." />
              )}
            </div>
          </div>

          <div className="library-sidebar-section">
            <div className="library-sidebar-title">Chủ đề</div>
            <div className="library-tree-list compact">
              {topics.length ? (
                topics.map((item) => (
                  <LibraryTreeButton
                    key={item.topicID}
                    active={topicID === item.topicID}
                    label={item.topicName || item.topicID}
                    meta={item.topicID}
                    onClick={() => handleSelectTopic(item.topicID)}
                  />
                ))
              ) : (
                <EmptyState title="Chưa có chủ đề" text="Chọn sách để tải danh sách chủ đề." />
              )}
            </div>
          </div>

          <div className="library-sidebar-section">
            <div className="library-sidebar-title">Bài học</div>
            <div className="library-tree-list compact">
              {lessons.length ? (
                lessons.map((item) => (
                  <LibraryTreeButton
                    key={item.lessonID}
                    active={lessonID === item.lessonID}
                    label={item.lessonName || item.lessonID}
                    meta={item.lessonID}
                    onClick={() => handleSelectLesson(item.lessonID, sort)}
                  />
                ))
              ) : (
                <EmptyState title="Chưa có bài học" text="Chọn chủ đề để tải các bài học." />
              )}
            </div>
          </div>
        </aside>

        <section className="library-content-pane">
          <div className="library-toolbar">
            <div className="library-toolbar-head">
              <div>
                <div className="library-toolbar-title">
                  {viewMode === "content"
                    ? selectedLesson?.lessonName || "Tài liệu bài học"
                    : viewMode === "lessons"
                    ? selectedTopic?.topicName || "Danh sách bài học"
                    : viewMode === "topics"
                    ? selectedSubject?.subjectName || "Danh sách chủ đề"
                    : viewMode === "subjects"
                    ? selectedClass?.className || "Danh sách sách"
                    : "Bắt đầu từ lớp học"}
                </div>
                <div className="library-toolbar-subtitle">
                  {viewMode === "content"
                    ? "Lọc tài liệu, hình ảnh và video trong bài đang chọn."
                    : viewMode === "lessons"
                    ? "Mở một bài học để xem toàn bộ nội dung bên trong."
                    : viewMode === "topics"
                    ? "Chọn chủ đề để đi tiếp xuống các bài học."
                    : viewMode === "subjects"
                    ? "Chọn sách hoặc môn học để tải danh sách chủ đề."
                    : "Thư viện hiển thị theo cây để bạn duyệt từng nhánh rõ ràng hơn."}
                </div>
              </div>
              <button className="btn" type="button" onClick={() => navigate("/user/search")}>
                Tới tìm kiếm
              </button>
            </div>

            <div className="library-toolbar-controls">
              <input
                className="library-filter-input"
                value={localQuery}
                onChange={(e) => setLocalQuery(e.target.value)}
                placeholder={
                  viewMode === "content"
                    ? "Lọc theo tên tài liệu, mô tả, keyword..."
                    : viewMode === "lessons"
                    ? "Lọc tên bài học..."
                    : viewMode === "topics"
                    ? "Lọc tên chủ đề..."
                    : viewMode === "subjects"
                    ? "Lọc tên sách hoặc môn học..."
                    : "Chọn lớp ở cột trái để bắt đầu"
                }
                disabled={viewMode === "classes"}
              />

              <select className="library-sort-select" value={sort} onChange={(e) => handleChangeSort(e.target.value)}>
                <option value="name">Tên A → Z</option>
                <option value="newest">Mới nhất</option>
                <option value="oldest">Cũ nhất</option>
                <option value="updated">Cập nhật gần đây</option>
              </select>
            </div>
          </div>

          {error ? <div className="library-inline-error">Lỗi: {error}</div> : null}
          {loading ? <div className="library-inline-note">Đang tải dữ liệu...</div> : null}

          {viewMode === "classes" ? (
            <div className="library-browse-grid two-col">
              {classes.map((item) => (
                <BrowseTile
                  key={item.classID}
                  title={item.className || item.classID}
                  subtitle="Mở lớp để xem sách, chủ đề và bài học."
                  badge={item.classID}
                  onOpen={() => handleSelectClass(item.classID)}
                />
              ))}
            </div>
          ) : null}

          {viewMode === "subjects" ? (
            filteredSubjects.length ? (
              <div className="library-browse-grid">
                {filteredSubjects.map((item, index) => (
                  <BrowseTile
                    key={item.subjectID}
                    title={item.subjectName || item.subjectID}
                    subtitle="Mở sách để xem danh sách chủ đề."
                    badge={`Sách ${index + 1}`}
                    onOpen={() => handleSelectSubject(item.subjectID)}
                  />
                ))}
              </div>
            ) : (
              <EmptyState title="Không có sách phù hợp" text="Thử đổi lớp hoặc xóa bộ lọc hiện tại." />
            )
          ) : null}

          {viewMode === "topics" ? (
            filteredTopics.length ? (
              <div className="library-browse-grid">
                {filteredTopics.map((item, index) => (
                  <BrowseTile
                    key={item.topicID}
                    title={item.topicName || item.topicID}
                    subtitle="Mở chủ đề để xem danh sách bài học."
                    badge={`Chủ đề ${index + 1}`}
                    onOpen={() => handleSelectTopic(item.topicID)}
                  />
                ))}
              </div>
            ) : (
              <EmptyState title="Không có chủ đề phù hợp" text="Thử đổi sách hoặc xóa bộ lọc hiện tại." />
            )
          ) : null}

          {viewMode === "lessons" ? (
            filteredLessons.length ? (
              <div className="library-browse-grid">
                {filteredLessons.map((item, index) => (
                  <BrowseTile
                    key={item.lessonID}
                    title={item.lessonName || item.lessonID}
                    subtitle="Mở bài để xem tài liệu, ảnh và video liên quan."
                    badge={`Bài ${index + 1}`}
                    onOpen={() => handleSelectLesson(item.lessonID, sort)}
                  />
                ))}
              </div>
            ) : (
              <EmptyState title="Không có bài học phù hợp" text="Thử xóa bộ lọc hoặc chọn chủ đề khác." />
            )
          ) : null}

          {viewMode === "content" ? (
            <>
              <div className="library-tab-row">
                <TabButton active={contentTab === "all"} onClick={() => setContentTab("all")} count={tabCounts.all}>
                  Tất cả
                </TabButton>
                <TabButton active={contentTab === "document"} onClick={() => setContentTab("document")} count={tabCounts.document}>
                  Tài liệu
                </TabButton>
                <TabButton active={contentTab === "image"} onClick={() => setContentTab("image")} count={tabCounts.image}>
                  Hình ảnh
                </TabButton>
                <TabButton active={contentTab === "video"} onClick={() => setContentTab("video")} count={tabCounts.video}>
                  Video
                </TabButton>
              </div>

              {(contentTab === "all" || contentTab === "document") && (
                <div className="library-block">
                  <div className="library-block-head">
                    <strong>Tài liệu bài học</strong>
                    <span>Mở nhanh file học liệu thuộc bài đang chọn.</span>
                  </div>
                  {filteredChunks.length ? (
                    <div className="library-card-stack">
                      {filteredChunks.map((doc) => (
                        <DocumentCard key={doc.chunkID} doc={doc} onToggleSave={handleToggleSave} />
                      ))}
                    </div>
                  ) : (
                    <EmptyState title="Chưa có tài liệu" text="Bài học này hiện chưa có tài liệu hiển thị." />
                  )}
                </div>
              )}

              {(contentTab === "all" || contentTab === "image") && (
                <div className="library-block">
                  <div className="library-block-head">
                    <strong>Hình ảnh liên quan</strong>
                    <span>Các ảnh được gắn cùng bài học để mở trực tiếp.</span>
                  </div>
                  {imageItems.length ? (
                    <div className="library-card-stack compact">
                      {imageItems.map((doc) => (
                        <DocumentCard key={`img-${doc.chunkID}`} doc={doc} onToggleSave={handleToggleSave} />
                      ))}
                    </div>
                  ) : (
                    <EmptyState title="Chưa có hình ảnh" text="Bài học này chưa có ảnh hiển thị." />
                  )}
                </div>
              )}

              {(contentTab === "all" || contentTab === "video") && (
                <div className="library-block">
                  <div className="library-block-head">
                    <strong>Video liên quan</strong>
                    <span>Các video đi kèm bài học hiện tại.</span>
                  </div>
                  {videoItems.length ? (
                    <div className="library-card-stack compact">
                      {videoItems.map((doc) => (
                        <DocumentCard key={`video-${doc.chunkID}`} doc={doc} onToggleSave={handleToggleSave} />
                      ))}
                    </div>
                  ) : (
                    <EmptyState title="Chưa có video" text="Bài học này chưa có video hiển thị." />
                  )}
                </div>
              )}
            </>
          ) : null}
        </section>
      </div>
    </div>
  );
}
