export type CsvRow = Record<string, string>;

export function parseCsv(text: string): { headers: string[]; rows: CsvRow[] } {
  const rows: string[][] = [];
  let cur: string[] = [];
  let field = "";
  let i = 0;
  let inQuotes = false;

  const pushField = (): void => {
    cur.push(field);
    field = "";
  };

  const pushRow = (): void => {
    if (cur.length === 1 && cur[0] === "" && rows.length === 0) return;
    rows.push(cur);
    cur = [];
  };

  while (i < text.length) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === "\"") {
        if (text[i + 1] === "\"") {
          field += "\"";
          i += 2;
          continue;
        }
        inQuotes = false;
        i += 1;
        continue;
      }
      field += ch;
      i += 1;
      continue;
    }

    if (ch === "\"") {
      inQuotes = true;
      i += 1;
      continue;
    }
    if (ch === ",") {
      pushField();
      i += 1;
      continue;
    }
    if (ch === "\n") {
      pushField();
      pushRow();
      i += 1;
      continue;
    }
    if (ch === "\r") {
      if (text[i + 1] === "\n") {
        pushField();
        pushRow();
        i += 2;
        continue;
      }
      pushField();
      pushRow();
      i += 1;
      continue;
    }
    field += ch;
    i += 1;
  }

  pushField();
  if (cur.length > 1 || cur[0] !== "" || rows.length > 0) pushRow();

  if (rows.length > 1) {
    const last = rows[rows.length - 1] ?? [];
    const isBlankTail = last.every((v) => (v ?? "").trim().length === 0);
    if (isBlankTail) rows.pop();
  }

  if (rows.length === 0) return { headers: [], rows: [] };
  const headers = rows[0].map((h) => h.trim());
  const outRows: CsvRow[] = [];
  for (let r = 1; r < rows.length; r += 1) {
    const arr = rows[r];
    const isEmptyRow = arr.every((v) => (v ?? "").trim().length === 0);
    if (isEmptyRow) continue;
    const obj: CsvRow = {};
    for (let c = 0; c < headers.length; c += 1) {
      obj[headers[c]] = (arr[c] ?? "").toString();
    }
    outRows.push(obj);
  }
  return { headers, rows: outRows };
}

export function toSnakeCase(value: string): string {
  return (value ?? "")
    .trim()
    .replace(/[^0-9A-Za-z]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}
