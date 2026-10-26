import { SemanticError } from "../../errors.js";
import { nameKey } from "../../schema/index.js";
import type { CTENode, WithClause } from "../parser/index.js";
import type { logical } from "../plan/index.js";
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
  for (const cte of clause.ctes) {
    checkNameUnused(cte.name, ctes);
    const isRecursive = clause.isRecursive && mentions(cte.query, cte.name);
    const bound = isRecursive
      ? bindRecursiveCte(cte, current)
      : bindCte(cte, current);
    ctes.push(bound);
    current = current.withCte(bound.definition);
  }
  return { scope: current, ctes };
}

/** The query's plan inside a `cte` node for each WITH query, the first outermost. */
export function wrapCtes(
  query: BoundQuery,
  ctes: readonly BoundCte[],
): BoundQuery {
  const plan = ctes.reduceRight<logical.Plan>(
    (body, cte) => ({
      kind: "cte",
      cteIndex: cte.definition.cteIndex,
      name: cte.definition.name,
      definition: cte.plan,
      body,
    }),
    query.plan,
  );
  return { ...query, plan };
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

function checkNameUnused(name: string, ctes: readonly BoundCte[]): void {
  const isUsed = ctes.some(
    (cte) => nameKey(cte.definition.name) === nameKey(name),
  );
  if (isUsed) {
    throw new SemanticError(`WITH query name ${name} is used twice`);
  }
}
