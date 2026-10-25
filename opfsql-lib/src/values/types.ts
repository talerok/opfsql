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
  return TYPES_BY_NAME.get(base.trim().toUpperCase()) ?? null;
}

/** The type a name stands for, ANY for a name opfsql does not know. */
export function sqlTypeOf(name: string): SqlType {
  return typeFromName(name) ?? "ANY";
}

/**
 * True for the names of INTEGER and REAL types (`BIGINT` included), which
 * compare and convert as numbers.
 */
export function isNumericType(name: string): boolean {
  const type = typeFromName(name);
  return type === "INTEGER" || type === "REAL";
}
