import type { SelectStatement } from "../parser/index.js";
import { bindWith, wrapCtes } from "./cte.js";
import { applyLimit } from "./limit.js";
import type { BoundQuery, Scope } from "./scope.js";
import { bindSelect } from "./select.js";
import { bindCompound } from "./set-operation.js";

// ---------------------------------------------------------------------------
// A query: WITH, a body — one SELECT or a compound — then ORDER BY and
// LIMIT over the whole body. `scope` is the query's own: the caller makes
// it a subquery's level, a sibling for FROM and WITH, or the statement's.
// ---------------------------------------------------------------------------

export function bindQuery(
  statement: SelectStatement,
  scope: Scope,
): BoundQuery {
  const { scope: inner, ctes } = bindWith(statement.with, scope);
  const query = bindBody(statement, inner);
  const limited = applyLimit(query, statement.limit, scope.context);
  return wrapCtes(limited, ctes);
}

/** The body, one SELECT or a compound, with the ORDER BY over it. */
function bindBody(statement: SelectStatement, scope: Scope): BoundQuery {
  const { body, orderBy } = statement;
  if (body.type === "SELECT_NODE") return bindSelect(body, scope, orderBy);
  return bindCompound(body, scope, orderBy);
}
