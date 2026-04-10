import type {
  ParsedExpression,
  ComparisonExpression,
  ConjunctionExpression,
  JoinRef,
} from '../../parser/types.js';
import { ExpressionClass, ExpressionType, JoinType } from '../../parser/types.js';
import type * as BT from '../types.js';
import { LogicalOperatorType, BoundExpressionClass } from '../types.js';
import type { BindContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { BindError } from '../core/errors.js';
import { mapComparisonType } from '../core/type-map.js';
import { checkTypeCompatibility } from '../core/type-check.js';
import { checkNoAggregates } from '../expression/aggregate.js';
import { bindExpression } from '../expression/index.js';
import { bindTableRef } from './index.js';

export function bindJoinRef(
  ctx: BindContext,
  ref: JoinRef,
  scope: BindScope,
): BT.LogicalOperator {
  const bindingsBefore = scope.getAllBindings().length;
  const left = bindTableRef(ctx, ref.left, scope);
  const bindingsAfterLeft = scope.getAllBindings().length;
  const right = bindTableRef(ctx, ref.right, scope);

  if (ref.join_type === JoinType.CROSS) {
    return {
      type: LogicalOperatorType.LOGICAL_CROSS_PRODUCT,
      children: [left, right],
      expressions: [],
      types: [...left.types, ...right.types],
      estimatedCardinality: 0,
      getColumnBindings: () => [
        ...left.getColumnBindings(),
        ...right.getColumnBindings(),
      ],
    } satisfies BT.LogicalCrossProduct;
  }

  if (ref.join_type === JoinType.RIGHT) {
    throw new BindError('RIGHT JOIN is not supported');
  }

  const joinType: 'INNER' | 'LEFT' =
    ref.join_type === JoinType.LEFT ? 'LEFT' : 'INNER';

  let conditions: BT.JoinCondition[] = [];

  if (ref.using_columns.length > 0) {
    const allBindings = scope.getAllBindings();
    const leftEntries = allBindings.slice(bindingsBefore, bindingsAfterLeft);
    const rightEntries = allBindings.slice(bindingsAfterLeft);

    for (const colName of ref.using_columns) {
      conditions.push({
        left: scope.resolveColumnIn(colName, leftEntries),
        right: scope.resolveColumnIn(colName, rightEntries),
        comparisonType: 'EQUAL',
      });
    }
  } else if (ref.condition) {
    checkNoAggregates(ref.condition, 'JOIN ON clause');
    conditions = extractJoinConditions(ctx, ref.condition, scope);
  }

  return {
    type: LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    joinType,
    children: [left, right],
    conditions,
    expressions: [],
    types: [...left.types, ...right.types],
    estimatedCardinality: 0,
    getColumnBindings: () => [
      ...left.getColumnBindings(),
      ...right.getColumnBindings(),
    ],
  } satisfies BT.LogicalComparisonJoin;
}

export function extractJoinConditions(
  ctx: BindContext,
  expr: ParsedExpression,
  scope: BindScope,
): BT.JoinCondition[] {
  if (expr.expression_class === ExpressionClass.CONJUNCTION) {
    const conj = expr as ConjunctionExpression;
    if (conj.type === ExpressionType.CONJUNCTION_AND) {
      const conditions: BT.JoinCondition[] = [];
      for (const child of conj.children) {
        conditions.push(...extractJoinConditions(ctx, child, scope));
      }
      return conditions;
    }
  }

  if (expr.expression_class === ExpressionClass.COMPARISON) {
    const cmp = expr as ComparisonExpression;
    const left = bindExpression(ctx, cmp.left, scope);
    const right = bindExpression(ctx, cmp.right, scope);
    checkTypeCompatibility(left.returnType, right.returnType);
    return [{ left, right, comparisonType: mapComparisonType(cmp.type) }];
  }

  const bound = bindExpression(ctx, expr, scope);
  return [
    {
      left: bound,
      right: {
        expressionClass: BoundExpressionClass.BOUND_CONSTANT,
        value: true,
        returnType: 'BOOLEAN',
      } satisfies BT.BoundConstantExpression,
      comparisonType: 'EQUAL',
    },
  ];
}
