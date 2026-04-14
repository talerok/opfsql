import type {
  BoundExpression,
  BoundFunctionExpression,
} from "../../binder/types.js";
import { ExecutorError } from "../errors.js";
import { compareValues } from "./utils/compare.js";
import { castText } from "./utils/cast.js";
import { likeToRegex } from "./utils/like.js";
import type { Resolver } from "../resolve.js";
import type { Tuple, Value } from "../types.js";
import type { SyncEvalContext } from "./context.js";
import { evaluateExpression } from "./index.js";

export function evalFunction(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
): Value {
  const fn = expr as BoundFunctionExpression;
  const name = fn.functionName.toUpperCase();
  const args: Value[] = [];
  for (const c of fn.children) {
    args.push(evaluateExpression(c, tuple, resolver, ctx));
  }

  switch (name) {
    case "UPPER":
      return args[0] === null ? null : requireText(args[0], "UPPER").toUpperCase();
    case "LOWER":
      return args[0] === null ? null : requireText(args[0], "LOWER").toLowerCase();
    case "LENGTH":
      if (args[0] === null) return null;
      if (args[0] instanceof Uint8Array) return args[0].length;
      return String(args[0]).length;
    case "TRIM":
      return args[0] === null ? null : requireText(args[0], "TRIM").trim();
    case "LTRIM":
      return args[0] === null ? null : requireText(args[0], "LTRIM").trimStart();
    case "RTRIM":
      return args[0] === null ? null : requireText(args[0], "RTRIM").trimEnd();
    case "SUBSTR":
    case "SUBSTRING":
      return evalSubstr(args);
    case "REPLACE":
      return evalReplace(args);
    case "CONCAT":
      return args.some((a) => a === null) ? null : args.map((a) => castText(a!)).join("");
    case "ABS":
      return args[0] === null ? null : Math.abs(args[0] as number);
    case "ROUND":
      return evalRound(args);
    case "FLOOR":
      return args[0] === null ? null : Math.floor(args[0] as number);
    case "CEIL":
    case "CEILING":
      return args[0] === null ? null : Math.ceil(args[0] as number);
    case "COALESCE":
      return args.find((a) => a !== null) ?? null;
    case "NULLIF":
      if (args[0] === null || args[1] === null) return args[0];
      return compareValues(args[0], args[1]) === 0 ? null : args[0];
    case "LIKE":
      return evalLike(args);
    case "NOT_LIKE":
      return evalNotLike(args);
    case "TYPEOF":
      if (args[0] === null) return "null";
      if (args[0] instanceof Uint8Array) return "blob";
      if (typeof args[0] === "object") return "json";
      return typeof args[0];
    default:
      return null;
  }
}

function requireText(v: Value, fnName: string): string {
  if (v instanceof Uint8Array) {
    throw new ExecutorError(`Cannot apply ${fnName} to BLOB value`);
  }
  if (typeof v === "object" && v !== null) {
    throw new ExecutorError(`Cannot apply ${fnName} to JSON value`);
  }
  return String(v);
}

function evalSubstr(args: Value[]): Value {
  if (args[0] === null || args[1] === null) return null;
  const str = requireText(args[0], "SUBSTR");
  const start = (args[1] as number) - 1;
  if (args.length >= 3 && args[2] !== null)
    return str.substring(start, start + (args[2] as number));
  return str.substring(start);
}

function evalReplace(args: Value[]): Value {
  if (args[0] === null || args[1] === null || args[2] === null) return null;
  return requireText(args[0], "REPLACE").replaceAll(String(args[1]), String(args[2]));
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
  requireText(args[0], "LIKE");
  return likeToRegex(String(args[1])).test(String(args[0]));
}

function evalNotLike(args: Value[]): Value {
  if (args[0] === null || args[1] === null) return null;
  requireText(args[0], "LIKE");
  return !likeToRegex(String(args[1])).test(String(args[0]));
}
