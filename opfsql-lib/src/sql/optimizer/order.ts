import type { logical, physical } from "../plan/index.js";
import { chooseAccessPath } from "./access-path.js";
import { isNeverTrue, substitute } from "./expression-tree.js";
import {
  lowerExpressions,
  lowerOptional,
  lowerOrderKeys,
  underProjects,
} from "./lower-expression.js";
import {
  projectedBy,
  projectsOf,
  type TableRead,
  tableReadOf,
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
  const keys = lowerOrderKeys(order.keys, planner);
  const rows = limit === null ? null : lowerLimit(limit, planner);
  const count = rows?.limit ?? null;
  const ordered = orderedRead(order, count !== null, planner);
  if (ordered !== null) return withLimit(ordered, rows);
  const child = lowerChild(order.child);
  if (rows !== null && count !== null) {
    return { kind: "top-n", child, keys, limit: count, offset: rows.offset };
  }
  return withLimit({ kind: "sort", child, keys }, rows);
}

interface LoweredLimit {
  readonly limit: physical.Expression | null;
  readonly offset: physical.Expression | null;
}

function lowerLimit(limit: logical.Limit, planner: Planner): LoweredLimit {
  return {
    limit: lowerOptional(limit.limit, planner),
    offset: lowerOptional(limit.offset, planner),
  };
}

function withLimit(
  plan: physical.Plan,
  limit: LoweredLimit | null,
): physical.Plan {
  if (limit === null) return plan;
  return { kind: "limit", child: plan, ...limit };
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
  if (read.conditions.some((condition) => isNeverTrue(condition))) return null;
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

/** `expression` over the outermost project's columns, rewritten over the innermost's child. */
function throughProjects(
  expression: logical.Expression,
  projects: readonly logical.Project[],
): logical.Expression | null {
  let result: logical.Expression | null = expression;
  for (const project of projects) {
    if (result === null) return null;
    result = substitute(result, projectedBy(project));
  }
  return result;
}
