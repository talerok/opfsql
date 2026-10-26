import type { Cast, physical } from "../sql/plan/index.js";
import {
  castValue,
  copyValue,
  isMutable,
  truthOf,
  type Value,
} from "../values/index.js";
import { compileOperation } from "./arithmetic.js";
import { compileCase } from "./case.js";
import { compileColumn, compileJsonAccess } from "./column.js";
import { compileFunction } from "./function.js";
import { compileCondition } from "./logic.js";
import { parameterValue } from "./parameter.js";
import type { CompiledExpression, Compiler, Scope, Tuple } from "./scope.js";
import { compileSubquery } from "./subquery.js";

// ---------------------------------------------------------------------------
// The expression compiler: the one way opfsql evaluates an expression. The
// tree is walked once, when its plan node is compiled for a run; what runs
// per row is a closure that already knows where its columns are and what
// its operators and functions do. Closures, never generated source: a
// strict Content Security Policy forbids `new Function`.
//
// A statement's plan is compiled for each run (docs/planning.md), so the
// run's parameters are read while compiling, like constants: `x IN ($1, $2)`
// hashes its list and `LIKE $1` builds its matcher once per run.
// ---------------------------------------------------------------------------

/** A condition compiled for one layout: whether it holds for a row. */
export type CompiledPredicate = (row: Tuple) => boolean;

/** Compiles `expression` for rows of `scope.layout`. */
export function compileExpression(
  expression: physical.Expression,
  scope: Scope,
): CompiledExpression {
  const compiler: Compiler = {
    scope,
    compile: (child) => compileNode(child, compiler),
  };
  return compiler.compile(expression);
}

/**
 * Compiles a condition (WHERE, ON, HAVING, a residual): true only when it
 * is TRUE — not FALSE, not NULL. A value that is not BOOLEAN is a
 * DATATYPE_MISMATCH, never judged by truthiness.
 */
export function compilePredicate(
  expression: physical.Expression,
  scope: Scope,
): CompiledPredicate {
  const condition = compileExpression(expression, scope);
  return (row) => truthOf(condition(row)) === true;
}

function compileNode(
  expression: physical.Expression,
  compiler: Compiler,
): CompiledExpression {
  switch (expression.kind) {
    case "column": {
      return compileColumn(expression, compiler.scope);
    }
    case "constant": {
      return compileConstant(expression.value);
    }
    case "parameter": {
      const value = parameterValue(compiler.scope.run, expression);
      return () => value;
    }
    case "comparison":
    case "conjunction":
    case "not":
    case "is-null":
    case "in-list":
    case "between": {
      return compileCondition(expression, compiler);
    }
    case "arithmetic":
    case "negate":
    case "concatenate": {
      return compileOperation(expression, compiler);
    }
    case "function": {
      return compileFunction(expression, compiler);
    }
    case "case": {
      return compileCase(expression, compiler);
    }
    case "cast": {
      return compileCast(expression, compiler);
    }
    case "json-access": {
      return compileJsonAccess(expression, compiler.scope);
    }
    case "scalar-subquery":
    case "exists":
    case "quantified-comparison": {
      return compileSubquery(expression, compiler);
    }
  }
}

/**
 * A constant's value. A BLOB or a JSON object or array is copied each time:
 * a cached plan keeps the constant, and a caller may change what it was
 * given.
 */
function compileConstant(value: Value): CompiledExpression {
  if (isMutable(value)) return () => copyValue(value);
  return () => value;
}

function compileCast(
  expression: Cast<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const operand = compiler.compile(expression.operand);
  const { type } = expression;
  return (row) => castValue(operand(row), type);
}
