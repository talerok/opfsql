import type {
  ParsedExpression,
  ColumnRefExpression,
  ConstantExpression,
  ParameterExpression,
  ComparisonExpression,
  ConjunctionExpression,
  OperatorExpression,
  BetweenExpression,
  FunctionExpression,
  SubqueryExpression,
  CaseExpression,
  CastExpression,
  StarExpression,
} from '../../parser/types.js';
import { ExpressionClass } from '../../parser/types.js';
import type { BoundExpression } from '../types.js';
import type { BindContext, AggregateContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { BindError } from '../core/errors.js';
import { bindColumnRef } from './column-ref.js';
import { bindConstant } from './constant.js';
import { bindParameter } from './parameter.js';
import { bindComparison } from './comparison.js';
import { bindConjunction } from './conjunction.js';
import { bindOperator } from './operator.js';
import { bindBetween } from './between.js';
import { bindFunction } from './function.js';
import { bindSubquery } from './subquery.js';
import { bindCase } from './case.js';
import { bindCast } from './cast.js';

export function bindExpression(
  ctx: BindContext,
  expr: ParsedExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundExpression {
  switch (expr.expression_class) {
    case ExpressionClass.COLUMN_REF:
      return bindColumnRef(expr as ColumnRefExpression, scope, aggCtx);
    case ExpressionClass.CONSTANT:
      return bindConstant(expr as ConstantExpression);
    case ExpressionClass.PARAMETER:
      return bindParameter(expr as ParameterExpression);
    case ExpressionClass.COMPARISON:
      return bindComparison(ctx, expr as ComparisonExpression, scope, aggCtx);
    case ExpressionClass.CONJUNCTION:
      return bindConjunction(ctx, expr as ConjunctionExpression, scope, aggCtx);
    case ExpressionClass.OPERATOR:
      return bindOperator(ctx, expr as OperatorExpression, scope, aggCtx);
    case ExpressionClass.BETWEEN:
      return bindBetween(ctx, expr as BetweenExpression, scope, aggCtx);
    case ExpressionClass.FUNCTION:
      return bindFunction(ctx, expr as FunctionExpression, scope, aggCtx);
    case ExpressionClass.SUBQUERY:
      return bindSubquery(ctx, expr as SubqueryExpression, scope);
    case ExpressionClass.CASE:
      return bindCase(ctx, expr as CaseExpression, scope, aggCtx);
    case ExpressionClass.CAST:
      return bindCast(ctx, expr as CastExpression, scope, aggCtx);
    case ExpressionClass.STAR:
      throw new BindError('Star expression must be handled in SELECT list');
    default:
      throw new BindError('Unknown expression class');
  }
}
