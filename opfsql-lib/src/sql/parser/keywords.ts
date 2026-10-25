// ---------------------------------------------------------------------------
// Keywords, as SQLite and PostgreSQL split them.
//
// A reserved keyword is never a name: a table, column or alias called
// `order` must be written "order". A non-reserved keyword is a name wherever
// the grammar expects one (`CREATE TABLE t (key TEXT, first TEXT)`,
// `left(s, 2)`), but never a bare alias: `FROM t1 NATURAL JOIN t2` must not
// read NATURAL as an alias of t1. Some keywords (OVER, FILTER, WINDOW,
// NATURAL, …) are here only so that they are not taken for aliases; the
// parser reports them as not supported.
//
// The reserved set is SQLite's, plus the words PostgreSQL also reserves that
// start an expression (CAST, TRUE, FALSE, CURRENT_*), minus INDEX, which
// PostgreSQL does not reserve and which is a common column name.
// ---------------------------------------------------------------------------

const RESERVED = [
  "ADD",
  "ALL",
  "ALTER",
  "AND",
  "AS",
  "AUTOINCREMENT",
  "BETWEEN",
  "CASE",
  "CAST",
  "CHECK",
  "COLLATE",
  "COMMIT",
  "CONSTRAINT",
  "CREATE",
  "CURRENT_DATE",
  "CURRENT_TIME",
  "CURRENT_TIMESTAMP",
  "DEFAULT",
  "DELETE",
  "DISTINCT",
  "DROP",
  "ELSE",
  "ESCAPE",
  "EXCEPT",
  "EXISTS",
  "FALSE",
  "FOREIGN",
  "FROM",
  "GROUP",
  "HAVING",
  "IN",
  "INSERT",
  "INTERSECT",
  "INTO",
  "IS",
  "ISNULL",
  "JOIN",
  "LIKE",
  "LIMIT",
  "NOT",
  "NOTHING",
  "NOTNULL",
  "NULL",
  "ON",
  "OR",
  "ORDER",
  "PRIMARY",
  "REFERENCES",
  "RETURNING",
  "SELECT",
  "SET",
  "TABLE",
  "THEN",
  "TRANSACTION",
  "TRUE",
  "UNION",
  "UNIQUE",
  "UPDATE",
  "USING",
  "VALUES",
  "WHEN",
  "WHERE",
  "WITH",
] as const;

const NON_RESERVED = [
  "ASC",
  "BEGIN",
  "BY",
  "COLUMN",
  "CONFLICT",
  "CROSS",
  "DESC",
  "DO",
  "END",
  "EXPLAIN",
  "FILTER",
  "FIRST",
  "FULL",
  "GENERATED",
  "GLOB",
  "IF",
  "INDEX",
  "INDEXED",
  "INNER",
  "KEY",
  "LAST",
  "LEFT",
  "MATCH",
  "NATURAL",
  "NULLS",
  "OFFSET",
  "OUTER",
  "OVER",
  "RECURSIVE",
  "REGEXP",
  "RIGHT",
  "ROLLBACK",
  "WINDOW",
] as const;

export type Keyword = (typeof RESERVED)[number] | (typeof NON_RESERVED)[number];

const KEYWORDS: ReadonlyMap<string, Keyword> = new Map<string, Keyword>(
  [...RESERVED, ...NON_RESERVED].map((keyword) => [keyword, keyword]),
);

const RESERVED_KEYWORDS: ReadonlySet<Keyword> = new Set<Keyword>(RESERVED);

/**
 * A word in upper case if it is made of ASCII letters, digits and
 * underscores, else undefined. Only such words spell keywords: the Unicode
 * case mapping would read `ſelect` as SELECT.
 */
export function upperAscii(word: string): string | undefined {
  return /^\w+$/.test(word) ? word.toUpperCase() : undefined;
}

/** The keyword a word spells, in any ASCII letter case, or undefined. */
export function keywordOf(word: string): Keyword | undefined {
  const upper = upperAscii(word);
  return upper === undefined ? undefined : KEYWORDS.get(upper);
}

export function isReserved(keyword: Keyword): boolean {
  return RESERVED_KEYWORDS.has(keyword);
}
