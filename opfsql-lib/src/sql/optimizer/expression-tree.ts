import {
  type ColumnBinding,
  type ColumnReference,
  type Constant,
  type Expression,
  isSubqueryExpression,
  nodesOf,
  rebuildChildren,
} from "../plan/index.js";

// ---------------------------------------------------------------------------
// Reading bound expressions, for every pass: which columns they read, and
// whether they may move. The walks and the rebuild are sql/plan's
// (walk.ts). Nothing here enters a subquery's plan: columns at depth 0 in
// there are the subquery's own, not this query's.
// ---------------------------------------------------------------------------

/** The columns of this query that `expression` reads. */
export function* columnsOf<Plan>(
  expression: Expression<Plan>,
): Generator<ColumnBinding> {
  for (const node of nodesOf(expression)) {
    if (node.kind === "column" && node.depth === 0) yield node.binding;
  }
}

/** True if `expression` reads a column of this query. */
export function readsAnyColumn<Plan>(expression: Expression<Plan>): boolean {
  const [first] = columnsOf(expression);
  return first !== undefined;
}

/** True if every column of this query that `expression` reads is in `tables`. */
export function readsOnly<Plan>(
  expression: Expression<Plan>,
  tables: ReadonlySet<number>,
): boolean {
  for (const binding of columnsOf(expression)) {
    if (!tables.has(binding.tableIndex)) return false;
  }
  return true;
}

/** The table indexes of a layout. */
export function tablesOf(layout: readonly ColumnBinding[]): Set<number> {
  return new Set(layout.map((binding) => binding.tableIndex));
}

/**
 * True if moving `expression` could change what it gives: it calls a
 * volatile function, whose every call differs, or holds a correlated
 * subquery, whose plan reads this query's rows through `depth` and would
 * need rewriting to move. Such a condition stays where the binder put it.
 */
export function isPinned<Plan>(expression: Expression<Plan>): boolean {
  return callsVolatile(expression) || holdsCorrelatedSubquery(expression);
}

function callsVolatile<Plan>(expression: Expression<Plan>): boolean {
  for (const node of nodesOf(expression)) {
    if (node.kind === "function" && node.function.isVolatile === true) {
      return true;
    }
  }
  return false;
}

/** True if `expression` holds a subquery that reads this query's rows (or rows further out). */
export function holdsCorrelatedSubquery<Plan>(
  expression: Expression<Plan>,
): boolean {
  for (const node of nodesOf(expression)) {
    if (isSubqueryExpression(node) && node.isCorrelated) return true;
  }
  return false;
}

/** The conjuncts of a condition: the operands of its ANDs, flattened. */
export function conjunctsOf<Plan>(
  expression: Expression<Plan>,
): Expression<Plan>[] {
  const isAnd =
    expression.kind === "conjunction" && expression.operator === "AND";
  if (!isAnd) return [expression];
  return expression.operands.flatMap((operand) => conjunctsOf(operand));
}

/** The operands of a condition's ORs, flattened. */
export function disjunctsOf<Plan>(
  expression: Expression<Plan>,
): Expression<Plan>[] {
  const isOr =
    expression.kind === "conjunction" && expression.operator === "OR";
  if (!isOr) return [expression];
  return expression.operands.flatMap((operand) => disjunctsOf(operand));
}

/** The constant TRUE, FALSE or NULL as a condition. */
export function truth(value: boolean | null): Constant {
  return { kind: "constant", value, type: "BOOLEAN" };
}

/** True for a constant FALSE or NULL: a condition no row passes. */
function isNeverTrue<Plan>(expression: Expression<Plan>): boolean {
  if (expression.kind !== "constant") return false;
  return expression.value === false || expression.value === null;
}

/** True if some condition is never TRUE: no row passes them all. */
export function hasNeverTrue<Plan>(
  conditions: readonly Expression<Plan>[],
): boolean {
  return conditions.some((condition) => isNeverTrue(condition));
}

/** True for the constant TRUE. */
export function isConstantTrue<Plan>(expression: Expression<Plan>): boolean {
  return expression.kind === "constant" && expression.value === true;
}

/** True for the constant FALSE. */
export function isConstantFalse<Plan>(expression: Expression<Plan>): boolean {
  return expression.kind === "constant" && expression.value === false;
}

/** What a column of this query becomes in a substitution, if anything. */
export type Replacement<Plan> = (
  column: ColumnReference,
) => Expression<Plan> | undefined;

/**
 * `expression` with the columns of this query that `replacement` knows
 * replaced by what it gives — or null when one cannot be: a JSON access
 * reads a column, and cannot read any other expression.
 */
export function substitute<Plan>(
  expression: Expression<Plan>,
  replacement: Replacement<Plan>,
): Expression<Plan> | null {
  // A column further out belongs to another query: never replaced.
  const replace = (column: ColumnReference): Expression<Plan> | undefined =>
    column.depth === 0 ? replacement(column) : undefined;
  for (const node of nodesOf(expression)) {
    if (node.kind !== "json-access") continue;
    const column = replace(node.column);
    if (column !== undefined && column.kind !== "column") return null;
  }
  return replaceColumns(expression, replace);
}

/** `expression` with each column `replace` knows replaced, a JSON access's by a column. */
function replaceColumns<Plan>(
  expression: Expression<Plan>,
  replace: Replacement<Plan>,
): Expression<Plan> {
  if (expression.kind === "column") return replace(expression) ?? expression;
  if (expression.kind === "json-access") {
    const column = replace(expression.column);
    if (column?.kind !== "column") return expression;
    return { ...expression, column };
  }
  return rebuildChildren(expression, {
    expression: (child) => replaceColumns(child, replace),
    plan: (plan) => plan,
  });
}
