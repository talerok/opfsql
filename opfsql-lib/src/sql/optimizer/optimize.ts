import type { Schema } from "../../schema/index.js";
import {
  type Assignment,
  type ConflictAction,
  type logical,
  type OnConflict,
  type physical,
  ROWID_COLUMN,
  type Statement,
} from "../plan/index.js";
import { lowerExpression } from "./lower-expression.js";
import { lowerPlan } from "./lower.js";
import { IndexCatalog, type KeyBinder } from "./key-parts.js";
import type { Planner } from "./planner.js";
import { pruneBelow, pruneColumns } from "./prune.js";
import { pushDownFilters } from "./pushdown.js";
import { simplifyPlan } from "./simplify.js";

// ---------------------------------------------------------------------------
// The optimizer (docs/planning.md): a bound statement, its plans logical,
// becomes the same statement with physical plans. Each plan — a query's, a
// DML statement's source, a subquery's inside an expression — goes through
// the same passes: simplify (simplify.ts), push filters down (pushdown.ts),
// then lower to physical, choosing joins, access paths, order and MIN/MAX
// on the way (lower.ts), then prune the columns no node uses (prune.ts):
// all of a plan's own output is used, but for an UPDATE's or DELETE's
// source, of which the executor reads the rowid (statement.ts). Every pass
// returns a new tree; nothing given is changed, and the same input always
// gives the same plan. DDL holds no plan and passes through.
// ---------------------------------------------------------------------------

export function optimize(
  statement: Statement<logical.Plan>,
  schema: Schema,
  bindKey: KeyBinder | null = null,
): Statement<physical.Plan> {
  const planner: Planner = {
    indexes: new IndexCatalog(schema, bindKey),
    planSubquery: (plan) => planQuery(plan, planner),
  };
  return optimizeStatement(statement, planner);
}

function planQuery(plan: logical.Plan, planner: Planner): physical.Plan {
  return pruneBelow(lowered(plan, planner));
}

function lowered(plan: logical.Plan, planner: Planner): physical.Plan {
  return lowerPlan(pushDownFilters(simplifyPlan(plan)), planner);
}

/**
 * An UPDATE's or DELETE's source reading what finding its rows needs: the
 * rowid, which the executor finds by its binding (statement.ts) — an
 * UPDATE then reads each row by it — and what the WHERE reads.
 */
function rowidSource(
  plan: logical.Plan,
  tableIndex: number,
  planner: Planner,
): physical.Plan {
  const rowid = { tableIndex, columnIndex: ROWID_COLUMN };
  return pruneColumns(lowered(plan, planner), [rowid]);
}

function optimizeStatement(
  statement: Statement<logical.Plan>,
  planner: Planner,
): Statement<physical.Plan> {
  switch (statement.kind) {
    case "query": {
      return { ...statement, plan: planQuery(statement.plan, planner) };
    }
    case "delete": {
      const { source, tableIndex } = statement;
      return { ...statement, source: rowidSource(source, tableIndex, planner) };
    }
    case "insert": {
      return {
        ...statement,
        source: planQuery(statement.source, planner),
        onConflict: lowerOnConflict(statement.onConflict, planner),
      };
    }
    case "update": {
      const { source, tableIndex } = statement;
      return {
        ...statement,
        source: rowidSource(source, tableIndex, planner),
        assignments: lowerAssignments(statement.assignments, planner),
      };
    }
    case "create-table":
    case "create-index":
    case "drop":
    case "alter-table": {
      return statement;
    }
  }
}

function lowerOnConflict(
  onConflict: OnConflict<logical.Plan> | null,
  planner: Planner,
): OnConflict<physical.Plan> | null {
  if (onConflict === null) return null;
  return {
    ...onConflict,
    action: lowerConflictAction(onConflict.action, planner),
  };
}

function lowerConflictAction(
  action: ConflictAction<logical.Plan>,
  planner: Planner,
): ConflictAction<physical.Plan> {
  if (action.kind === "nothing") return action;
  const { condition } = action;
  return {
    ...action,
    assignments: lowerAssignments(action.assignments, planner),
    condition: condition === null ? null : lowerExpression(condition, planner),
  };
}

function lowerAssignments(
  assignments: readonly Assignment<logical.Plan>[],
  planner: Planner,
): Assignment<physical.Plan>[] {
  return assignments.map((assignment) => ({
    ...assignment,
    value: lowerExpression(assignment.value, planner),
  }));
}
