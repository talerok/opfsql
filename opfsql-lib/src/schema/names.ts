// ---------------------------------------------------------------------------
// Names of tables, columns and indexes. They are kept as written and compared
// without regard to the case of ASCII letters, as in SQLite: `Users` and
// `USERS` name the same table, `Été` and `été` two different ones.
// ---------------------------------------------------------------------------

/** Names starting with this are reserved for opfsql's own tables and indexes. */
export const RESERVED_PREFIX = "opfsql_";

/** The form two names share exactly when they are the same name. */
export function nameKey(name: string): string {
  return name.replaceAll(/[A-Z]/gu, (letter) => letter.toLowerCase());
}

export function isSameName(a: string, b: string): boolean {
  return nameKey(a) === nameKey(b);
}

export function isReservedName(name: string): boolean {
  return nameKey(name).startsWith(RESERVED_PREFIX);
}
