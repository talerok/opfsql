import { SemanticError } from "../../errors.js";
import { nameKey } from "../../schema/index.js";
import type { CTENode, WithClause } from "../parser/index.js";
import { cteColumns, type BoundCte } from "./cte-definition.js";
import { bindRecursiveCte } from "./recursive-cte.js";
import { mentions } from "./references.js";
import type { BoundQuery, Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// WITH. Each query is bound beside the query it serves, sees the ones
// written before it, and is a `cte` node around that query's plan, read by
// `cte-scan`s. In WITH RECURSIVE, a query that names itself is a recursive
// CTE (recursive-cte.ts).
// ---------------------------------------------------------------------------

/** The scope the query body sees, and its CTEs in the order written. */
export interface BoundWith {
  readonly scope: Scope;
  readonly ctes: readonly BoundCte[];
}

export function bindWith(clause: WithClause | null, scope: Scope): BoundWith {
  if (clause === null) return { scope, ctes: [] };
  const ctes: BoundCte[] = [];
  let current = scope;
  for (const [position, cte] of clause.ctes.entries()) {
    checkNameUnused(cte.name, clause.ctes.slice(0, position));
    const isRecursive = clause.isRecursive && mentions(cte.query, cte.name);
    const bound = isRecursive
      ? bindRecursive(cte, current)
      : [bindCte(cte, current)];
    ctes.push(...bound);
    const last = bound.at(-1);
    if (last !== undefined) current = current.withCte(last.definition);
  }
  return { scope: current, ctes };
}

/** The query's plan inside a `cte` node for each WITH query, the first outermost. */
export function wrapCtes(
  query: BoundQuery,
  ctes: readonly BoundCte[],
): BoundQuery {
  let plan = query.plan;
  for (const cte of ctes.toReversed()) {
    const { cteIndex, name } = cte.definition;
    plan = { kind: "cte", cteIndex, name, definition: cte.plan, body: plan };
  }
  return { ...query, plan };
}

/**
 * A recursive WITH query, after the WITH queries of its own WITH clause,
 * which its anchor and step see. Those cannot read the recursive query, so
 * their plans can wrap it from outside: it is the last of the list.
 */
function bindRecursive(cte: CTENode, scope: Scope): BoundCte[] {
  const nested = bindWith(cte.query.with, scope.sibling());
  return [...nested.ctes, bindRecursiveCte(cte, nested.scope)];
}

function bindCte(cte: CTENode, scope: Scope): BoundCte {
  const query = scope.context.bindQuery(cte.query, scope.sibling());
  const definition = {
    name: cte.name,
    cteIndex: scope.context.newIndex(),
    columns: cteColumns(cte, query.columns),
  };
  return { definition, plan: query.plan };
}

function checkNameUnused(name: string, before: readonly CTENode[]): void {
  const isUsed = before.some((cte) => nameKey(cte.name) === nameKey(name));
  if (isUsed) {
    throw new SemanticError(`WITH query name ${name} is used twice`);
  }
}
