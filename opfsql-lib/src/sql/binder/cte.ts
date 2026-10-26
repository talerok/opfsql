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
    const { nested, bound } = bindWithQuery(cte, clause.isRecursive, current);
    ctes.push(...nested, bound);
    current = current.withCte(bound.definition);
  }
  return { scope: current, ctes };
}

/** A WITH query, and the WITH queries of its own WITH that wrap it from outside. */
interface WithQuery {
  readonly nested: readonly BoundCte[];
  readonly bound: BoundCte;
}

function bindWithQuery(
  cte: CTENode,
  isRecursiveClause: boolean,
  scope: Scope,
): WithQuery {
  const isRecursive = isRecursiveClause && mentions(cte.query, cte.name);
  if (!isRecursive) return { nested: [], bound: bindCte(cte, scope) };
  const { nested, recursive } = bindRecursive(cte, scope);
  return { nested, bound: recursive };
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
 * their plans can wrap it from outside.
 */
function bindRecursive(
  cte: CTENode,
  scope: Scope,
): { readonly nested: readonly BoundCte[]; readonly recursive: BoundCte } {
  const nested = bindWith(cte.query.with, scope.sibling());
  const recursive = bindRecursiveCte(cte, nested.scope);
  return { nested: nested.ctes, recursive };
}

function bindCte(cte: CTENode, scope: Scope): BoundCte {
  const definitionScope = scope.sibling();
  const query = scope.context.bindQuery(cte.query, definitionScope);
  const definition = {
    name: cte.name,
    cteIndex: scope.context.newIndex(),
    columns: cteColumns(cte, query.columns),
    reach: definitionScope.reach,
  };
  return { definition, plan: query.plan };
}

function checkNameUnused(name: string, before: readonly CTENode[]): void {
  const isUsed = before.some((cte) => nameKey(cte.name) === nameKey(name));
  if (isUsed) {
    throw new SemanticError(`WITH query name ${name} is used twice`);
  }
}
