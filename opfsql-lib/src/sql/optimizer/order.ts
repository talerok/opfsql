import type { logical, physical } from "../plan/index.js";
import { chooseAccessPath } from "./access-path.js";
import { orderedCost } from "./cost.js";
import { columnsOf, hasNeverTrue } from "./expression-tree.js";
import { planJoin } from "./join.js";
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
// Lowering step, order (lower.ts; docs/planning.md, rule 3). An ORDER BY over
// a table read — through projects and the table's filter — asks the access
// path for its order (order-match.ts says when an index delivers it, and
// access-path.ts's rowidOrder when the rowid does). Only a path that delivers
// exactly that order lets the sort go. Over a join — through projects and a
// filter — an ORDER BY of one table's columns asks that table to lead in its
// order: a lookup join keeps its left side's order, so when every join after
// it is one, the sort goes too, if that costs less than joining freely and
// sorting (cost.ts). Otherwise a sort stays; with a LIMIT right above it, a
// top-n takes the place of both, and the limit is not kept beside it.
// ---------------------------------------------------------------------------

/** A table read and the projects above it, outermost first. */
interface ProjectedRead extends TableRead {
  readonly projects: readonly logical.Project[];
}

/** `order`'s rows in order, with `limit` above it if there is one. */
export function lowerOrder(
  order: logical.Order,
  limit: logical.Limit | null,
  planner: Planner,
): physical.Plan {
  const clause = limitClauseOf(limit, planner);
  const isLimited = countOf(clause) !== null;
  const ordered =
    orderedRead(order, isLimited, planner) ??
    orderedJoin(order, isLimited, planner);
  if (ordered !== null) return withLimit(ordered, clause);
  const keys = lowerOrderKeys(order.keys, planner);
  return sorted(planner.lower(order.child), keys, clause);
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
  const chain = projectedReadOf(order.child);
  const keys = chain === null ? null : keysThrough(order.keys, chain.projects);
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

/** A join under projects, and a filter over it. */
interface ProjectedJoin {
  readonly projects: readonly logical.Project[];
  readonly join: logical.Join;
  readonly conditions: readonly logical.Expression[];
}

/**
 * The order's child, a join, read in the order wanted: led by the table
 * every key reads, in that order, each join after it a lookup join (rule
 * 3); null when that cannot be, or costs more than a free join and a sort.
 */
function orderedJoin(
  order: logical.Order,
  isLimited: boolean,
  planner: Planner,
): physical.Plan | null {
  const chain = projectedJoinOf(order.child);
  const keys = chain === null ? null : keysThrough(order.keys, chain.projects);
  const tableIndex = keys === null ? null : onlyTable(keys);
  if (chain === null || keys === null || tableIndex === null) return null;
  const wanted = { keys: lowerOrderKeys(keys, planner), isLimited };
  const led = planJoin(chain.join, planner, { tableIndex, order: wanted });
  if (!led.isOrdered) return null;
  const free = planJoin(chain.join, planner, null);
  // Two guesses at the same join's rows, from either side: the tighter.
  const rows = Math.min(led.estimate.rows, free.estimate.rows);
  const ledCost = orderedCost(
    led.estimate,
    { isDelivered: true, isLimited },
    rows,
  );
  const freeCost = orderedCost(
    free.estimate,
    { isDelivered: false, isLimited },
    rows,
  );
  if (ledCost > freeCost) return null;
  const conditions = lowerExpressions(chain.conditions, planner);
  return underProjects(
    withFilter(led.plan, conditions),
    chain.projects,
    planner,
  );
}

function withFilter(
  plan: physical.Plan,
  conditions: readonly physical.Expression[],
): physical.Plan {
  if (conditions.length === 0) return plan;
  return { kind: "filter", child: plan, conditions };
}

function projectedJoinOf(plan: logical.Plan): ProjectedJoin | null {
  const { projects, below } = projectsOf(plan);
  if (below.kind === "join") return { projects, join: below, conditions: [] };
  const isFilteredJoin = below.kind === "filter" && below.child.kind === "join";
  if (!isFilteredJoin || hasNeverTrue(below.conditions)) return null;
  return { projects, join: below.child, conditions: below.conditions };
}

/** The one table whose columns every key reads, or null. */
function onlyTable(keys: readonly logical.OrderKey[]): number | null {
  const tables = new Set<number>();
  for (const key of keys) {
    for (const binding of columnsOf(key.expression)) {
      tables.add(binding.tableIndex);
    }
  }
  const [only = null, ...others] = tables;
  return others.length === 0 ? only : null;
}

function projectedReadOf(plan: logical.Plan): ProjectedRead | null {
  const { projects, below } = projectsOf(plan);
  const read = tableReadOf(below);
  if (read === null) return null;
  // A filter no row passes reads nothing; lowering it reads no index either.
  if (hasNeverTrue(read.conditions)) return null;
  return { projects, ...read };
}

/** The order keys read through `projects`; null if one cannot be. */
function keysThrough(
  keys: readonly logical.OrderKey[],
  projects: readonly logical.Project[],
): logical.OrderKey[] | null {
  const resolved: logical.OrderKey[] = [];
  for (const key of keys) {
    const expression = throughProjects(key.expression, projects);
    if (expression === null) return null;
    resolved.push({ ...key, expression });
  }
  return resolved;
}
