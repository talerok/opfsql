import type { BoundExpression, BoundOperatorExpression } from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';
import type { SyncEvalContext } from './context.js';
import { evaluateExpression } from './index.js';
import { compareValues } from './utils/compare.js';
import { castText } from './utils/cast.js';
import { ExecutorError } from '../errors.js';

export function evalOperator(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
): Value {
  const op = expr as BoundOperatorExpression;

  switch (op.operatorType) {
    case 'IS_NULL':
      return evaluateExpression(op.children[0], tuple, resolver, ctx) === null;
    case 'IS_NOT_NULL':
      return evaluateExpression(op.children[0], tuple, resolver, ctx) !== null;
    case 'NOT': {
      const val = evaluateExpression(op.children[0], tuple, resolver, ctx);
      if (val === null) return null;
      return !val;
    }
    case 'NEGATE': {
      const val = evaluateExpression(op.children[0], tuple, resolver, ctx);
      if (val === null) return null;
      return -(val as number);
    }
    case 'IN':
      return evalIn(op.children, tuple, resolver, ctx, false);
    case 'NOT_IN':
      return evalIn(op.children, tuple, resolver, ctx, true);
    case 'CONCAT': {
      const left = evaluateExpression(op.children[0], tuple, resolver, ctx);
      const right = evaluateExpression(op.children[1], tuple, resolver, ctx);
      if (left === null || right === null) return null;
      return castText(left) + castText(right);
    }
    case 'ADD':
    case 'SUBTRACT':
    case 'MULTIPLY':
    case 'DIVIDE':
    case 'MOD':
      return evalArithmetic(op.operatorType, op.children, tuple, resolver, ctx);
    default:
      throw new ExecutorError(`Unknown operator type: ${op.operatorType}`);
  }
}

function evalIn(
  children: BoundExpression[],
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
  negate: boolean,
): Value {
  const input = evaluateExpression(children[0], tuple, resolver, ctx);
  if (input === null) return null;

  let hasNull = false;
  for (let i = 1; i < children.length; i++) {
    const val = evaluateExpression(children[i], tuple, resolver, ctx);
    if (val === null) { hasNull = true; continue; }
    if (compareValues(input, val) === 0) return !negate;
  }
  return hasNull ? null : negate;
}

function evalArithmetic(
  op: string,
  children: BoundExpression[],
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
): Value {
  const left = evaluateExpression(children[0], tuple, resolver, ctx);
  const right = evaluateExpression(children[1], tuple, resolver, ctx);
  if (left === null || right === null) return null;

  const a = left as number;
  const b = right as number;

  switch (op) {
    case 'ADD': return a + b;
    case 'SUBTRACT': return a - b;
    case 'MULTIPLY': return a * b;
    case 'DIVIDE': return b === 0 ? null : a / b;
    case 'MOD': return b === 0 ? null : a % b;
    default: return null;
  }
}
