import type {
  BoundExpression,
  BoundOperatorExpression,
} from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';
import type { EvalContext } from './context.js';
import { evaluateExpression } from './index.js';
import { ExecutorError } from '../errors.js';

export async function evalOperator(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Promise<Value> {
  const op = expr as BoundOperatorExpression;

  switch (op.operatorType) {
    case 'IS_NULL':
      return (await evaluateExpression(op.children[0], tuple, resolver, ctx)) === null;
    case 'IS_NOT_NULL':
      return (await evaluateExpression(op.children[0], tuple, resolver, ctx)) !== null;
    case 'NOT':
      return evalNot(op.children[0], tuple, resolver, ctx);
    case 'NEGATE':
      return evalNegate(op.children[0], tuple, resolver, ctx);
    case 'IN':
      return evalIn(op.children, tuple, resolver, ctx, false);
    case 'NOT_IN':
      return evalIn(op.children, tuple, resolver, ctx, true);
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

async function evalNot(
  child: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Promise<Value> {
  const val = await evaluateExpression(child, tuple, resolver, ctx);
  if (val === null) return null;
  return !val;
}

async function evalNegate(
  child: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Promise<Value> {
  const val = await evaluateExpression(child, tuple, resolver, ctx);
  if (val === null) return null;
  return -(val as number);
}

async function evalIn(
  children: BoundExpression[],
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
  negate: boolean,
): Promise<Value> {
  const input = await evaluateExpression(children[0], tuple, resolver, ctx);
  if (input === null) return null;

  let hasNull = false;
  for (let i = 1; i < children.length; i++) {
    const val = await evaluateExpression(children[i], tuple, resolver, ctx);
    if (val === null) {
      hasNull = true;
      continue;
    }
    if (input === val) return !negate;
  }
  return hasNull ? null : negate;
}

async function evalArithmetic(
  op: string,
  children: BoundExpression[],
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Promise<Value> {
  const left = await evaluateExpression(children[0], tuple, resolver, ctx);
  const right = await evaluateExpression(children[1], tuple, resolver, ctx);
  if (left === null || right === null) return null;

  const a = left as number;
  const b = right as number;

  switch (op) {
    case 'ADD':
      return a + b;
    case 'SUBTRACT':
      return a - b;
    case 'MULTIPLY':
      return a * b;
    case 'DIVIDE':
      return b === 0 ? null : a / b;
    case 'MOD':
      return b === 0 ? null : a % b;
    default:
      return null;
  }
}
