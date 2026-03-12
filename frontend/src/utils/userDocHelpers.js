function normalizeText(value = "") {
  return String(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function isTinHocName(value = "") {
  const text = normalizeText(value);
  return text.includes("tin hoc") || text.includes("informat") || text === "it";
}

export function filterTinHocSubjects(items = []) {
  return (items || []).filter((item) => {
    const subjectName = item?.subjectName || item?.subjectID || item?.name || "";
    return isTinHocName(subjectName);
  });
}

export function pickTinHocSubject(items = []) {
  return filterTinHocSubjects(items)[0] || null;
}

export function filterTinHocDocuments(items = []) {
  return (items || []).filter((item) => {
    const subjectName =
      item?.subject?.subjectName ||
      item?.subject?.subjectID ||
      item?.subjectName ||
      "";
    return isTinHocName(subjectName);
  });
}