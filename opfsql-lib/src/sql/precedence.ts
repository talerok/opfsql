// ---------------------------------------------------------------------------
// SQL's operator precedence, in one place: the parser parses by it, and the
// plan printers (sql/plan) print by it, so a printed expression parses back
// to the tree it came from. It depends on nothing.
// ---------------------------------------------------------------------------

/**
 * The binding powers, loosest first: SQLite's precedence (parse.y).
 * Operators on one level associate to the left, so `a - b - c` is
 * `(a - b) - c` and `a = b IS NULL` is `(a = b) IS NULL`.
 */
export const power = {
  /** Any expression. */
  lowest: 0,
  or: 1,
  and: 2,
  /** Prefix NOT: `NOT a = b` is `NOT (a = b)`. */
  not: 3,
  /** `=`, `==`, `!=`, `<>`, IS, [NOT] IN, LIKE, BETWEEN, ISNULL, NOTNULL. */
  equality: 4,
  /** `<`, `<=`, `>`, `>=`. */
  relational: 5,
  /** `&`, `|`, `<<`, `>>`. */
  bitwise: 6,
  /** `+`, `-`. */
  additive: 7,
  /** `*`, `/`, `%`. */
  multiplicative: 8,
  /** `||`, `->`, `->>`. */
  concatenation: 9,
  /** COLLATE. */
  collation: 10,
  /** Prefix `-`, `+`, `~`. */
  unary: 11,
  /** What binds tighter than every operator: names, literals, calls, `( … )`. */
  primary: 12,
} as const;

/** Where the right side of a left-associative operator starts. */
export function above(level: number): number {
  return level + 1;
}
