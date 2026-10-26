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
import { isScan } from "./access-path.js";
import { columnsOf, holdsCorrelatedSubquery } from "./expression-tree.js";
import { lowerExpression } from "./lower-expression.js";
import { lowerPlan } from "./lower.js";
import type { KeyBinder, Planner } from "./planner.js";
import { pushDownFilters } from "./pushdown.js";
import { simplifyPlan } from "./simplify.js";

// ---------------------------------------------------------------------------
// The optimizer (docs/planning.md): a bound statement, its plans logical,
// becomes the same statement with physical plans. Each plan — a query's, a
// DML statement's source, a subquery's inside an expression — goes through
// the same passes: simplify (simplify.ts), push filters down (pushdown.ts),
// then lower to physical, choosing joins, access paths, order and MIN/MAX
// on the way (lower.ts). Every pass returns a new tree; nothing given is
// changed, and the same input always gives the same plan. DDL holds no
// plan and passes through.
// ---------------------------------------------------------------------------

export function optimize(
  statement: Statement<logical.Plan>,
  schema: Schema,
  bindKey: KeyBinder | null = null,
): Statement<physical.Plan> {
  const planner: Planner = {
    schema,
    bindKey,
    planSubquery: (plan) => planQuery(plan, planner),
  };
  return optimizeStatement(statement, planner);
}

function planQuery(plan: logical.Plan, planner: Planner): physical.Plan {
  return lowerPlan(pushDownFilters(simplifyPlan(plan)), planner);
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
      const source = planQuery(statement.source, planner);
      return { ...statement, source: rowidSource(source) };
    }
    case "insert": {
      return {
        ...statement,
        source: planQuery(statement.source, planner),
        onConflict: lowerOnConflict(statement.onConflict, planner),
      };
    }
    case "update": {
      return {
        ...statement,
        source: planQuery(statement.source, planner),
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

/**
 * A DELETE's source narrowed to what deleting needs: the rowid, which the
 * executor finds by its binding (statement.ts). Once the WHERE is in the
 * scan's residual, the scan reads only the rowid and the columns that
 * residual reads. A residual with a correlated subquery keeps every column:
 * the subquery's plan reads them from outside, where columnsOf cannot see.
 */
function rowidSource(plan: physical.Plan): physical.Plan {
  if (
    !isScan(plan) ||
    plan.residual.some((condition) => holdsCorrelatedSubquery(condition))
  ) {
    return plan;
  }
  const read = new Set<number>();
  for (const condition of plan.residual) {
    for (const binding of columnsOf(condition)) read.add(binding.columnIndex);
  }
  const columns = plan.columns.filter(
    (column) => column === ROWID_COLUMN || read.has(column),
  );
  return { ...plan, columns };
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
