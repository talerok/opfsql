import type {
  BoundExpression,
  BoundFunctionExpression,
} from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';
import type { EvalContext } from './context.js';
import { evaluateExpression } from './index.js';
import { likeToRegex, compareValues } from './helpers.js';

export async function evalFunction(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Promise<Value> {
  const fn = expr as BoundFunctionExpression;
  const name = fn.functionName.toUpperCase();
  const args: Value[] = [];
  for (const c of fn.children) {
    args.push(await evaluateExpression(c, tuple, resolver, ctx));
  }

  switch (name) {
    case 'UPPER':
      return args[0] === null ? null : String(args[0]).toUpperCase();
    case 'LOWER':
      return args[0] === null ? null : String(args[0]).toLowerCase();
    case 'LENGTH':
      return args[0] === null ? null : String(args[0]).length;
    case 'TRIM':
      return args[0] === null ? null : String(args[0]).trim();
    case 'LTRIM':
      return args[0] === null ? null : String(args[0]).trimStart();
    case 'RTRIM':
      return args[0] === null ? null : String(args[0]).trimEnd();
    case 'SUBSTR':
    case 'SUBSTRING':
      return evalSubstr(args);
    case 'REPLACE':
      return evalReplace(args);
    case 'CONCAT':
      return args.some((a) => a === null) ? null : args.map(String).join('');
    case 'ABS':
      return args[0] === null ? null : Math.abs(args[0] as number);
    case 'ROUND':
      return evalRound(args);
    case 'FLOOR':
      return args[0] === null ? null : Math.floor(args[0] as number);
    case 'CEIL':
    case 'CEILING':
      return args[0] === null ? null : Math.ceil(args[0] as number);
    case 'COALESCE':
      return args.find((a) => a !== null) ?? null;
    case 'NULLIF':
      if (args[0] === null || args[1] === null) return args[0];
      return compareValues(args[0], args[1]) === 0 ? null : args[0];
    case 'LIKE':
      return evalLike(args);
    case 'TYPEOF':
      return args[0] === null ? 'null' : typeof args[0];
    default:
      return null;
  }
}

function evalSubstr(args: Value[]): Value {
  if (args[0] === null || args[1] === null) return null;
  const str = String(args[0]);
  const start = (args[1] as number) - 1; // SQL is 1-based
  if (args.length >= 3 && args[2] !== null) {
    return str.substring(start, start + (args[2] as number));
  }
  return str.substring(start);
}

function evalReplace(args: Value[]): Value {
  if (args[0] === null || args[1] === null || args[2] === null) return null;
  return String(args[0]).replaceAll(String(args[1]), String(args[2]));
}

function evalRound(args: Value[]): Value {
  if (args[0] === null) return null;
  const precision =
    args.length >= 2 && args[1] !== null ? (args[1] as number) : 0;
  const factor = Math.pow(10, precision);
  return Math.round((args[0] as number) * factor) / factor;
}

function evalLike(args: Value[]): Value {
  if (args[0] === null || args[1] === null) return null;
  return likeToRegex(String(args[1])).test(String(args[0]));
}
