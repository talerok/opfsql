// Column and expression types (docs/semantics.md, Column types).

export type SqlType =
  "INTEGER" | "REAL" | "TEXT" | "BLOB" | "BOOLEAN" | "JSON" | "ANY";

const TYPES_BY_NAME: ReadonlyMap<string, SqlType> = new Map<string, SqlType>([
  ["INTEGER", "INTEGER"],
  ["INT", "INTEGER"],
  ["BIGINT", "INTEGER"],
  ["SMALLINT", "INTEGER"],
  ["TINYINT", "INTEGER"],
  ["REAL", "REAL"],
  ["FLOAT", "REAL"],
  ["DOUBLE", "REAL"],
  ["TEXT", "TEXT"],
  ["VARCHAR", "TEXT"],
  ["CHAR", "TEXT"],
  ["BLOB", "BLOB"],
  ["BOOLEAN", "BOOLEAN"],
  ["BOOL", "BOOLEAN"],
  ["JSON", "JSON"],
  ["ANY", "ANY"],
]);

/**
 * The type a declared name stands for — `varchar(255)` is TEXT — or null if
 * the name is not a type opfsql knows.
 */
export function typeFromName(name: string): SqlType | null {
  const parameters = name.indexOf("(");
  const base = parameters === -1 ? name : name.slice(0, parameters);
  return TYPES_BY_NAME.get(asciiUpperCase(base.trim())) ?? null;
}

/** True for INTEGER and REAL, which compare and convert as numbers. */
export function isNumericType(type: SqlType): boolean {
  return type === "INTEGER" || type === "REAL";
}

/** Type names are keywords: only ASCII letters change case, as in SQLite. */
function asciiUpperCase(name: string): string {
  return name.replaceAll(/[a-z]/gu, (letter) => letter.toUpperCase());
}
