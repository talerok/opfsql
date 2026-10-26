import type { logical } from "../plan/index.js";
import { isPinned } from "./expression-tree.js";
import { projectsOf, throughProjects } from "./plan-tree.js";

// ---------------------------------------------------------------------------
// LIMIT and OFFSET below projections. A project keeps rows one for one, so a
// limit over it may as well sit under it: the select list — a correlated
// subquery in it too — is then computed only for the rows returned, never
// for the rows OFFSET skips. Under an ORDER BY over projects, the order goes
// below them with the limit when its keys can be computed there: each key,
// read through the projects, must not call a volatile function or hold a
// correlated subquery, or sorting by it and showing it would see two
// values. Only projects are passed: a sort, a DISTINCT or an aggregate
// needs every row.
// ---------------------------------------------------------------------------

/** A limit placed as low as projects allow, and the projects to put back above it. */
interface PlacedLimit {
  /** Outermost first, as they were. */
  readonly projects: readonly logical.Project[];
  readonly limit: logical.Limit;
}

export function placeLimit(limit: logical.Limit): PlacedLimit {
  const outer = projectsOf(limit.child);
  const { below } = outer;
  if (below.kind === "order") {
    const sunk = orderBelowProjects(below);
    const projects = [...outer.projects, ...sunk.projects];
    return { projects, limit: { ...limit, child: sunk.order } };
  }
  return { projects: outer.projects, limit: { ...limit, child: below } };
}

/** The order moved below the projects it sorts, when its keys allow it. */
function orderBelowProjects(order: logical.Order): {
  readonly projects: readonly logical.Project[];
  readonly order: logical.Order;
} {
  const { projects, below } = projectsOf(order.child);
  const keys: logical.OrderKey[] = [];
  for (const key of order.keys) {
    const expression = throughProjects(key.expression, projects);
    if (expression === null || isPinned(expression)) {
      return { projects: [], order };
    }
    keys.push({ ...key, expression });
  }
  return { projects, order: { ...order, child: below, keys } };
}
