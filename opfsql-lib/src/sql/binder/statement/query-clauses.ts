import type { SelectStatement } from "../../parser/types.js";

/** The clauses of a query that apply to its whole body: WITH, ORDER BY, LIMIT. */
export type QueryClauses = Pick<SelectStatement, "with" | "orderBy" | "limit">;

/** For a body bound inside a compound query, which has none of its own. */
export const NO_CLAUSES: QueryClauses = {
  with: null,
  orderBy: [],
  limit: null,
};
