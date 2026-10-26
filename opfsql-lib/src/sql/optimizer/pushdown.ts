import { InternalError } from "../../errors.js";
import type { ColumnReference, logical } from "../plan/index.js";
import { logical as plans } from "../plan/index.js";
import {
  columnsOf,
  hasNeverTrue,
  isPinned,
  readsOnly,
  substitute,
  tablesOf,
} from "./expression-tree.js";
import { projectedBy } from "./plan-tree.js";

// ---------------------------------------------------------------------------
// Pass 2, filter pushdown: each conjunct of a filter moves down the plan as
// far as it keeps its meaning, so it is checked on fewer rows and reaches
// the scan whose access path can use it (access-path.ts).
//
//   project        substituting the projected expressions for its columns,
//                  unless that puts a volatile call or a JSON access of a
//                  non-column in the condition;
//   inner join     into the side whose columns it reads alone; a WHERE
//                  conjunct reading both sides joins the ON conditions;
//   left join      a WHERE conjunct into the left side when it reads only
//                  that; an ON conjunct into the right side when it reads
//                  only that. A WHERE conjunct never enters the right side
//                  or the ON: it must see the NULLs of unmatched rows;
//   aggregate      below it when it reads group keys alone and there are
//                  groups (with none, an empty input still makes one row);
//   UNION [ALL]    into both sides, when every column it reads has the same
//                  type on both sides as in the result, so no conversion
//                  changes what it compares;
//   order, distinct, the body of a CTE: through them unchanged.
//
// A conjunct that calls a volatile function or holds a correlated subquery
// (isPinned) never moves: it stays directly above the node it was above,
// where it runs exactly as often as before — after the conjuncts that stay
// there with it, so a cheap test spares it rows. A filter that is never
// TRUE is left alone: it reads nothing however deep the others go.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;
type Plan = logical.Plan;

export function pushDownFilters(plan: Plan): Plan {
  return pushInto(plan, []);
}

/** `plan` with `conditions` applied on top, each as deep as it can go. */
function pushInto(plan: Plan, conditions: readonly Expression[]): Plan {
  const movable = conditions.filter((condition) => !isPinned(condition));
  const pinned = conditions.filter((condition) => isPinned(condition));
  return withFilter(place(plan, movable), pinned);
}

/** `plan` under a filter of `conditions`, merged into one it already has. */
function withFilter(plan: Plan, conditions: readonly Expression[]): Plan {
  if (conditions.length === 0) return plan;
  if (plan.kind === "filter") {
    return { ...plan, conditions: [...plan.conditions, ...conditions] };
  }
  return { kind: "filter", child: plan, conditions };
}

/** `plan` with movable `conditions` on top, placed by the rules above. */
function place(plan: Plan, conditions: readonly Expression[]): Plan {
  switch (plan.kind) {
    case "filter": {
      if (hasNeverTrue(plan.conditions)) {
        return plan;
      }
      return pushInto(plan.child, [...plan.conditions, ...conditions]);
    }
    case "project": {
      return placeThroughProject(plan, conditions);
    }
    case "join": {
      return plan.joinType === "inner"
        ? placeIntoInnerJoin(plan, conditions)
        : placeIntoLeftJoin(plan, conditions);
    }
    case "aggregate": {
      return placeBelowAggregate(plan, conditions);
    }
    case "set-operation": {
      return placeIntoSetOperation(plan, conditions);
    }
    case "order":
    case "distinct": {
      return { ...plan, child: pushInto(plan.child, conditions) };
    }
    case "cte": {
      const definition = pushDownFilters(plan.definition);
      return { ...plan, definition, body: pushInto(plan.body, conditions) };
    }
    case "limit": {
      return withFilter(
        { ...plan, child: pushDownFilters(plan.child) },
        conditions,
      );
    }
    case "recursive-cte": {
      const anchor = pushDownFilters(plan.anchor);
      const step = pushDownFilters(plan.step);
      return withFilter({ ...plan, anchor, step }, conditions);
    }
    case "get":
    case "values":
    case "cte-scan": {
      return withFilter(plan, conditions);
    }
  }
}

/** Conditions split into those that go below a node and those that stay above. */
interface Placement {
  readonly below: Expression[];
  readonly above: Expression[];
}

/**
 * Each condition rewritten for below a node by `rewrite`, or kept above when
 * it gives null. A rewrite that brings in a volatile call stays above.
 */
function split(
  conditions: readonly Expression[],
  rewrite: (condition: Expression) => Expression | null,
): Placement {
  const placement: Placement = { below: [], above: [] };
  for (const condition of conditions) {
    const rewritten = rewrite(condition);
    if (rewritten === null || isPinned(rewritten)) {
      placement.above.push(condition);
    } else {
      placement.below.push(rewritten);
    }
  }
  return placement;
}

function placeThroughProject(
  plan: logical.Project,
  conditions: readonly Expression[],
): Plan {
  const { below, above } = split(conditions, (condition) =>
    substitute(condition, projectedBy(plan)),
  );
  return withFilter({ ...plan, child: pushInto(plan.child, below) }, above);
}

function placeBelowAggregate(
  plan: logical.Aggregate,
  conditions: readonly Expression[],
): Plan {
  const hasGroups = plan.groups.length > 0;
  const groupTables = new Set([plan.groupIndex]);
  const { below, above } = split(conditions, (condition) => {
    if (!hasGroups || !readsOnly(condition, groupTables)) return null;
    return substitute(condition, (column) =>
      column.binding.tableIndex === plan.groupIndex
        ? plan.groups[column.binding.columnIndex]
        : undefined,
    );
  });
  return withFilter({ ...plan, child: pushInto(plan.child, below) }, above);
}

function placeIntoInnerJoin(
  plan: logical.Join,
  conditions: readonly Expression[],
): Plan {
  if (hasNeverTrue(plan.conditions)) {
    return withFilter(withSidesPushed(plan, [], []), conditions);
  }
  const left = tablesOf(plans.layoutOf(plan.left));
  const right = tablesOf(plans.layoutOf(plan.right));
  const toLeft: Expression[] = [];
  const toRight: Expression[] = [];
  const kept: Expression[] = [];
  for (const condition of [...plan.conditions, ...conditions]) {
    if (isPinned(condition)) kept.push(condition);
    else if (readsOnly(condition, left)) toLeft.push(condition);
    else if (readsOnly(condition, right)) toRight.push(condition);
    else kept.push(condition);
  }
  return { ...withSidesPushed(plan, toLeft, toRight), conditions: kept };
}

function placeIntoLeftJoin(
  plan: logical.Join,
  conditions: readonly Expression[],
): Plan {
  const left = tablesOf(plans.layoutOf(plan.left));
  const right = tablesOf(plans.layoutOf(plan.right));
  const toLeft = conditions.filter((condition) => readsOnly(condition, left));
  const above = conditions.filter((condition) => !readsOnly(condition, left));
  const isEmptyOn = hasNeverTrue(plan.conditions);
  const isRightOnly = (condition: Expression): boolean =>
    !isEmptyOn && !isPinned(condition) && readsOnly(condition, right);
  const toRight = plan.conditions.filter((condition) => isRightOnly(condition));
  const kept = plan.conditions.filter((condition) => !isRightOnly(condition));
  const join = { ...withSidesPushed(plan, toLeft, toRight), conditions: kept };
  return withFilter(join, above);
}

function withSidesPushed(
  plan: logical.Join,
  toLeft: readonly Expression[],
  toRight: readonly Expression[],
): logical.Join {
  const left = pushInto(plan.left, toLeft);
  return { ...plan, left, right: pushInto(plan.right, toRight) };
}

function placeIntoSetOperation(
  plan: logical.SetOperation,
  conditions: readonly Expression[],
): Plan {
  const isUnion = plan.operator === "UNION" || plan.operator === "UNION ALL";
  const isPassable = (condition: Expression): boolean =>
    isUnion && readsSameTypes(plan, condition);
  const passing = conditions.filter((condition) => isPassable(condition));
  const above = conditions.filter((condition) => !isPassable(condition));
  const left = pushInto(
    plan.left,
    passing.map((condition) => onSide(condition, plan, plan.left)),
  );
  const right = pushInto(
    plan.right,
    passing.map((condition) => onSide(condition, plan, plan.right)),
  );
  return withFilter({ ...plan, left, right }, above);
}

/** True if each result column `condition` reads has the result's type on both sides. */
function readsSameTypes(
  plan: logical.SetOperation,
  condition: Expression,
): boolean {
  const leftTypes = plans.typesOf(plan.left);
  const rightTypes = plans.typesOf(plan.right);
  for (const binding of columnsOf(condition)) {
    if (binding.tableIndex !== plan.tableIndex) return false;
    const type = plan.types[binding.columnIndex];
    const isSame =
      leftTypes[binding.columnIndex] === type &&
      rightTypes[binding.columnIndex] === type;
    if (!isSame) return false;
  }
  return true;
}

/** `condition` over the result of a set operation, rewritten over one side. */
function onSide(
  condition: Expression,
  plan: logical.SetOperation,
  side: Plan,
): Expression {
  const layout = plans.layoutOf(side);
  const types = plans.typesOf(side);
  const rewritten = substitute(
    condition,
    (column): ColumnReference | undefined => {
      if (column.binding.tableIndex !== plan.tableIndex) return undefined;
      const binding = layout[column.binding.columnIndex];
      const type = types[column.binding.columnIndex];
      if (binding === undefined || type === undefined) return undefined;
      return { ...column, binding, type };
    },
  );
  // Column for column, so a JSON access still reads a column.
  if (rewritten === null) {
    throw new InternalError("a set operation's column became no column");
  }
  return rewritten;
}
