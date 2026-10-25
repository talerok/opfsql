import { MisuseError } from "../../errors.js";
import {
  type BoundExpression,
  BoundExpressionClass,
  type IndexHint,
  type LogicalOperator,
  LogicalOperatorType,
  type TableFilter,
} from "../../sql/binder/types.js";
import { normalizeValue, type Value } from "../../values/index.js";

// ---------------------------------------------------------------------------
// Parameters are checked once per statement, before anything runs: a
// statement that uses $3 needs at least three, wherever $3 appears and
// whether or not a row ever reaches it. Values are normalized on the way in
// (NaN is NULL, -0 is 0), as every value from outside is.
// ---------------------------------------------------------------------------

/** The statement's parameters, checked against `plan` and normalized. */
export function checkParameters(
  plan: LogicalOperator,
  parameters: readonly Value[],
): Value[] {
  const needed = parameterCount(plan);
  if (parameters.length < needed) {
    throw new MisuseError(
      `the statement uses ${String(needed)} parameter${needed === 1 ? "" : "s"}, but ${String(parameters.length)} ${parameters.length === 1 ? "was" : "were"} given`,
    );
  }
  return parameters.map((value) => normalizeValue(value));
}

/** One more than the largest parameter index in the plan, or 0. */
export function parameterCount(plan: LogicalOperator): number {
  let count = 0;
  const visit = (expression: BoundExpression): void => {
    if (expression.expressionClass === BoundExpressionClass.BOUND_PARAMETER) {
      count = Math.max(count, expression.index + 1);
    }
    if (expression.expressionClass === BoundExpressionClass.BOUND_SUBQUERY) {
      count = Math.max(count, parameterCount(expression.subplan));
    }
    for (const child of childExpressions(expression)) visit(child);
  };
  for (const expression of operatorExpressions(plan)) visit(expression);
  for (const child of plan.children) {
    count = Math.max(count, parameterCount(child));
  }
  return count;
}

/** Every expression a plan node holds, not those of its children. */
function operatorExpressions(operator: LogicalOperator): BoundExpression[] {
  return [...operator.expressions, ...extraExpressions(operator)];
}

/** The expressions a plan node holds outside `expressions`. */
function extraExpressions(operator: LogicalOperator): BoundExpression[] {
  switch (operator.type) {
    case LogicalOperatorType.LOGICAL_GET: {
      return [
        ...filterExpressions(operator.tableFilters),
        ...hintExpressions(operator.indexHint),
      ];
    }
    case LogicalOperatorType.LOGICAL_COMPARISON_JOIN: {
      return operator.conditions.flatMap((condition) => [
        condition.left,
        condition.right,
      ]);
    }
    case LogicalOperatorType.LOGICAL_ORDER_BY: {
      return operator.orders.map((order) => order.expression);
    }
    case LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY: {
      return [...operator.groups, ...present(operator.havingExpression)];
    }
    case LogicalOperatorType.LOGICAL_INSERT: {
      const conflict = operator.onConflict;
      return conflict === undefined
        ? []
        : [...conflict.updateExpressions, ...present(conflict.whereExpression)];
    }
    case LogicalOperatorType.LOGICAL_FILTER:
    case LogicalOperatorType.LOGICAL_PROJECTION:
    case LogicalOperatorType.LOGICAL_CROSS_PRODUCT:
    case LogicalOperatorType.LOGICAL_LIMIT:
    case LogicalOperatorType.LOGICAL_DISTINCT:
    case LogicalOperatorType.LOGICAL_UNION:
    case LogicalOperatorType.LOGICAL_UPDATE:
    case LogicalOperatorType.LOGICAL_DELETE:
    case LogicalOperatorType.LOGICAL_CREATE_TABLE:
    case LogicalOperatorType.LOGICAL_CREATE_INDEX:
    case LogicalOperatorType.LOGICAL_ALTER_TABLE:
    case LogicalOperatorType.LOGICAL_DROP:
    case LogicalOperatorType.LOGICAL_CTE_REF:
    case LogicalOperatorType.LOGICAL_MATERIALIZED_CTE:
    case LogicalOperatorType.LOGICAL_RECURSIVE_CTE: {
      return [];
    }
  }
}

function present(
  expression: BoundExpression | null | undefined,
): BoundExpression[] {
  return expression === null || expression === undefined ? [] : [expression];
}

function filterExpressions(filters: readonly TableFilter[]): BoundExpression[] {
  return filters.flatMap((filter) => [filter.expression, filter.constant]);
}

function hintExpressions(hint: IndexHint | undefined): BoundExpression[] {
  if (hint === undefined) return [];
  if (hint.kind === "union") {
    return [
      hint.originalFilter,
      ...hint.branches.flatMap((branch) => hintExpressions(branch)),
    ];
  }
  return [
    ...hint.predicates.map((predicate) => predicate.value),
    ...filterExpressions(hint.residualFilters),
    ...filterExpressions(hint.coveredFilters),
  ];
}

/** The direct subexpressions of `expression` (not inside subqueries). */
function childExpressions(expression: BoundExpression): BoundExpression[] {
  switch (expression.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF:
    case BoundExpressionClass.BOUND_CONSTANT:
    case BoundExpressionClass.BOUND_PARAMETER:
    case BoundExpressionClass.BOUND_JSON_ACCESS: {
      return [];
    }
    case BoundExpressionClass.BOUND_COMPARISON: {
      return [expression.left, expression.right];
    }
    case BoundExpressionClass.BOUND_CONJUNCTION:
    case BoundExpressionClass.BOUND_OPERATOR:
    case BoundExpressionClass.BOUND_FUNCTION:
    case BoundExpressionClass.BOUND_AGGREGATE: {
      return expression.children;
    }
    case BoundExpressionClass.BOUND_BETWEEN: {
      return [expression.input, expression.lower, expression.upper];
    }
    case BoundExpressionClass.BOUND_SUBQUERY: {
      return present(expression.child);
    }
    case BoundExpressionClass.BOUND_CASE: {
      return [
        ...present(expression.operand),
        ...expression.caseChecks.flatMap((check) => [check.when, check.result]),
        ...present(expression.elseExpr),
      ];
    }
    case BoundExpressionClass.BOUND_CAST: {
      return [expression.child];
    }
  }
}
