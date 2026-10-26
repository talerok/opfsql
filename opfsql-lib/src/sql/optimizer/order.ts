import type { logical, physical } from "../plan/index.js";
import { chooseAccessPath } from "./access-path.js";
import { hasNeverTrue } from "./expression-tree.js";
import {
  lowerExpressions,
  lowerOptional,
  lowerOrderKeys,
  underProjects,
} from "./lower-expression.js";
import {
  projectsOf,
  type TableRead,
  tableReadOf,
  throughProjects,
} from "./plan-tree.js";
import type { Planner } from "./planner.js";

// ---------------------------------------------------------------------------
// Pass 5, order (docs/planning.md, rule 3). An ORDER BY over a table read —
// through projects and the table's filter — asks the access path for its
// order (order-match.ts says when an index or the rowid delivers it). Only
// a path that delivers exactly that order lets the sort go. Otherwise a
// sort stays; with a LIMIT right above it, a top-n takes the place of both,
// and the limit is not kept beside it.
// ---------------------------------------------------------------------------

/** A table read and the projects above it, outermost first. */
interface ScanChain extends TableRead {
  readonly projects: readonly logical.Project[];
}

/** `order`'s rows in order, with `limit` above it if there is one. */
export function lowerOrder(
  order: logical.Order,
  limit: logical.Limit | null,
  planner: Planner,
  lowerChild: (plan: logical.Plan) => physical.Plan,
): physical.Plan {
  const clause = limitClauseOf(limit, planner);
  const ordered = orderedRead(order, countOf(clause) !== null, planner);
  if (ordered !== null) return withLimit(ordered, clause);
  const keys = lowerOrderKeys(order.keys, planner);
  return sorted(lowerChild(order.child), keys, clause);
}

/** A LIMIT's count and offset, lowered. */
interface LimitClause {
  readonly count: physical.Expression | null;
  readonly offset: physical.Expression | null;
}

function limitClauseOf(
  limit: logical.Limit | null,
  planner: Planner,
): LimitClause | null {
  if (limit === null) return null;
  const count = lowerOptional(limit.limit, planner);
  return { count, offset: lowerOptional(limit.offset, planner) };
}

function withLimit(
  plan: physical.Plan,
  clause: LimitClause | null,
): physical.Plan {
  if (clause === null) return plan;
  const { count, offset } = clause;
  return { kind: "limit", child: plan, limit: count, offset };
}

/** The LIMIT's count, or null for none: no LIMIT, or an OFFSET alone. */
function countOf(clause: LimitClause | null): physical.Expression | null {
  return clause?.count ?? null;
}

/** A sort; with a LIMIT count above it, a top-n in place of both. */
function sorted(
  child: physical.Plan,
  keys: readonly physical.OrderKey[],
  clause: LimitClause | null,
): physical.Plan {
  const count = countOf(clause);
  if (count === null) return withLimit({ kind: "sort", child, keys }, clause);
  const offset = clause?.offset ?? null;
  return { kind: "top-n", child, keys, limit: count, offset };
}

/**
 * The order's child read in the order wanted, through an access path that
 * delivers it; null when no path does, or the child is no table read.
 */
function orderedRead(
  order: logical.Order,
  isLimited: boolean,
  planner: Planner,
): physical.Plan | null {
  const chain = scanChainOf(order.child);
  const keys = chain === null ? null : keysOverTable(order.keys, chain);
  if (chain === null || keys === null) return null;
  const path = chooseAccessPath(
    {
      get: chain.get,
      conditions: lowerExpressions(chain.conditions, planner),
      order: { keys: lowerOrderKeys(keys, planner), isLimited },
    },
    planner,
  );
  if (!path.isOrdered) return null;
  return underProjects(path.plan, chain.projects, planner);
}

function scanChainOf(plan: logical.Plan): ScanChain | null {
  const { projects, below } = projectsOf(plan);
  const read = tableReadOf(below);
  if (read === null) return null;
  // A filter no row passes reads nothing; lowering it reads no index either.
  if (hasNeverTrue(read.conditions)) return null;
  return { projects, ...read };
}

/** The order keys over the table's columns, read through the projects; null if one cannot be. */
function keysOverTable(
  keys: readonly logical.OrderKey[],
  chain: ScanChain,
): logical.OrderKey[] | null {
  const resolved: logical.OrderKey[] = [];
  for (const key of keys) {
    const expression = throughProjects(key.expression, chain.projects);
    if (expression === null) return null;
    resolved.push({ ...key, expression });
  }
  return resolved;
}
