import { SemanticError } from "../errors.js";
import type {
  Exists,
  physical,
  QuantifiedComparison,
  ScalarSubquery,
} from "../sql/plan/index.js";
import {
  foldTruth,
  membership,
  type Truth,
  type Value,
} from "../values/index.js";
import { compareTyped, comparisonConversion } from "./comparison.js";
import type {
  CompiledExpression,
  CompiledSubquery,
  Compiler,
  QueryContext,
  Scope,
  Tuple,
} from "./scope.js";

// ---------------------------------------------------------------------------
// Subqueries in expressions (docs/planning.md, Execution). The executor
// compiles each subquery's plan once, with the expression; a correlated one
// runs again for each row, with that row on the outer stack, and any other
// runs the first time it is needed and keeps its rows.
// ---------------------------------------------------------------------------

export type Subquery = Extract<
  physical.Expression,
  { readonly kind: "scalar-subquery" | "exists" | "quantified-comparison" }
>;

/** A subquery's rows for the row its expression is evaluated on. */
type CompiledRows = (row: Tuple) => readonly Tuple[];

export function compileSubquery(
  expression: Subquery,
  compiler: Compiler,
): CompiledExpression {
  switch (expression.kind) {
    case "scalar-subquery": {
      return compileScalar(expression, compiler.scope);
    }
    case "exists": {
      return compileExists(expression, compiler.scope);
    }
    case "quantified-comparison": {
      return compileQuantified(expression, compiler);
    }
  }
}

/** Its one value; NULL without rows, and an error with several. */
function compileScalar(
  expression: ScalarSubquery<physical.Plan>,
  scope: Scope,
): CompiledExpression {
  const rows = compileRows(expression, scope, 2);
  return (row) => onlyValue(rows(row));
}

/** EXISTS needs one row to decide. */
function compileExists(
  expression: Exists<physical.Plan>,
  scope: Scope,
): CompiledExpression {
  const rows = compileRows(expression, scope, 1);
  const { isNegated } = expression;
  return (row) => {
    const hasRows = rows(row).length > 0;
    return hasRows !== isNegated;
  };
}

/**
 * `x <op> ANY (subquery)` is true if the comparison holds for some row,
 * `ALL` if it holds for every row; three-valued: a NULL comparison makes
 * the answer NULL unless another row decides it. No rows: ANY is false and
 * ALL is true, whatever x is — so `NULL IN (empty)` is false.
 */
function compileQuantified(
  expression: QuantifiedComparison<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  // `x IN (subquery)` is `x = ANY (subquery)`.
  const isIn =
    expression.operator === "EQUAL" && expression.quantifier === "ANY";
  const isHashable = isIn && !expression.isCorrelated;
  if (isHashable) return compileHashedIn(expression, compiler);
  const needle = compiler.compile(expression.needle);
  const rows = compileRows(expression, compiler.scope);
  const test = compareTyped(
    expression.operator,
    expression.needle.type,
    expression.columnType,
  );
  const kind = expression.quantifier === "ALL" ? "and" : "or";
  function* comparisons(
    value: Value,
    items: readonly Tuple[],
  ): Generator<Truth> {
    for (const [item = null] of items) yield test(value, item);
  }
  return (row) => foldTruth(kind, comparisons(needle(row), rows(row)));
}

/**
 * `x IN (subquery)` for a subquery that runs once: its values hashed once,
 * converted as `=` converts them, so each row costs one lookup.
 */
function compileHashedIn(
  expression: QuantifiedComparison<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const { needle, columnType } = expression;
  const needleValue = compiler.compile(needle);
  const convertNeedle = comparisonConversion(needle.type, columnType);
  const convertItem = comparisonConversion(columnType, needle.type);
  const subquery = compilePlan(expression, compiler.scope);
  const contains = once(() =>
    membership(subquery.rows().map(([item = null]) => convertItem(item))),
  );
  return (row) => {
    const value = convertNeedle(needleValue(row));
    return contains()(value);
  };
}

/**
 * The subquery's rows, at most `limit` of them: for a correlated subquery,
 * run within the row the expression is evaluated on; for any other, run
 * once, when first needed.
 */
function compileRows(
  expression: Subquery,
  scope: Scope,
  limit?: number,
): CompiledRows {
  const subquery = compilePlan(expression, scope);
  if (!expression.isCorrelated) return once(() => subquery.rows(limit));
  const { outer } = scope.run;
  return (row) => outer.within(row, () => subquery.rows(limit));
}

/**
 * The subquery's plan, compiled by the executor as a query level inside
 * `scope`: the rows of `scope.layout` are its depth-1 rows.
 */
function compilePlan(expression: Subquery, scope: Scope): CompiledSubquery {
  const context: QueryContext = {
    run: scope.run,
    outerLayouts: [scope.layout, ...scope.outerLayouts],
  };
  return scope.run.subqueries.compile(expression.plan, context);
}

function onlyValue(rows: readonly Tuple[]): Value {
  if (rows.length > 1) {
    throw new SemanticError("a scalar subquery returned more than one row");
  }
  return rows[0]?.[0] ?? null;
}

/** `compute`'s result, computed on the first call and kept. */
function once<T>(compute: () => T): () => T {
  let result: { readonly value: T } | undefined;
  return () => {
    result ??= { value: compute() };
    return result.value;
  };
}
