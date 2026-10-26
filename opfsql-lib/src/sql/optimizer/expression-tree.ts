import {
  type Between,
  type Case,
  type ColumnBinding,
  type ColumnReference,
  type Constant,
  type Expression,
  nodesOf,
} from "../plan/index.js";

// ---------------------------------------------------------------------------
// Rebuilding and reading bound expressions, for every pass; the walk
// itself is sql/plan's (walk.ts). Generic in the plan their subqueries
// hold, so one rebuild serves the logical rewrites and the lowering to
// physical plans. Nothing here enters a subquery's plan: columns at depth 0
// in there are the subquery's own, not this query's.
// ---------------------------------------------------------------------------

/** How to rebuild the parts of an expression: child expressions, subquery plans. */
export interface Rebuild<From, To> {
  readonly expression: (expression: Expression<From>) => Expression<To>;
  readonly plan: (plan: From) => To;
}

/** `expression` with each direct part rebuilt; leaves come back as they are. */
export function rebuildChildren<From, To>(
  expression: Expression<From>,
  rebuild: Rebuild<From, To>,
): Expression<To> {
  const child = rebuild.expression;
  const each = (expressions: readonly Expression<From>[]): Expression<To>[] =>
    expressions.map((item) => child(item));
  switch (expression.kind) {
    case "column":
    case "constant":
    case "parameter":
    case "json-access": {
      return expression;
    }
    case "comparison":
    case "arithmetic":
    case "concatenate": {
      const { left, right } = expression;
      return { ...expression, left: child(left), right: child(right) };
    }
    case "not":
    case "is-null":
    case "negate":
    case "cast": {
      return { ...expression, operand: child(expression.operand) };
    }
    case "conjunction": {
      return { ...expression, operands: each(expression.operands) };
    }
    case "in-list": {
      const { needle, list } = expression;
      return { ...expression, needle: child(needle), list: each(list) };
    }
    case "between": {
      return rebuildBetween(expression, child);
    }
    case "function": {
      return { ...expression, arguments: each(expression.arguments) };
    }
    case "case": {
      return rebuildCase(expression, rebuild);
    }
    case "scalar-subquery":
    case "exists":
    case "quantified-comparison": {
      return rebuildSubquery(expression, rebuild);
    }
  }
}

function rebuildBetween<From, To>(
  expression: Between<From>,
  child: (expression: Expression<From>) => Expression<To>,
): Between<To> {
  const { input, lower, upper } = expression;
  return {
    ...expression,
    input: child(input),
    lower: child(lower),
    upper: child(upper),
  };
}

function rebuildSubquery<From, To>(
  expression: Subquery<From>,
  rebuild: Rebuild<From, To>,
): Subquery<To> {
  const plan = rebuild.plan(expression.plan);
  if (expression.kind !== "quantified-comparison") {
    return { ...expression, plan };
  }
  return { ...expression, needle: rebuild.expression(expression.needle), plan };
}

function rebuildCase<From, To>(
  expression: Case<From>,
  rebuild: Rebuild<From, To>,
): Case<To> {
  const child = rebuild.expression;
  const { operand, branches, otherwise } = expression;
  return {
    ...expression,
    operand: operand === null ? null : child(operand),
    branches: branches.map(({ when, result }) => ({
      when: child(when),
      result: child(result),
    })),
    otherwise: otherwise === null ? null : child(otherwise),
  };
}

/** The columns of this query that `expression` reads. */
export function* columnsOf<Plan>(
  expression: Expression<Plan>,
): Generator<ColumnBinding> {
  for (const node of nodesOf(expression)) {
    if (node.kind === "column" && node.depth === 0) yield node.binding;
  }
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
    if (isSubquery(node) && node.isCorrelated) return true;
  }
  return false;
}

/** An expression holding a subquery's plan. */
type Subquery<Plan> = Extract<
  Expression<Plan>,
  { readonly kind: "scalar-subquery" | "exists" | "quantified-comparison" }
>;

function isSubquery<Plan>(
  expression: Expression<Plan>,
): expression is Subquery<Plan> {
  switch (expression.kind) {
    case "scalar-subquery":
    case "exists":
    case "quantified-comparison": {
      return true;
    }
    case "column":
    case "constant":
    case "parameter":
    case "comparison":
    case "conjunction":
    case "not":
    case "is-null":
    case "arithmetic":
    case "negate":
    case "concatenate":
    case "in-list":
    case "between":
    case "function":
    case "case":
    case "cast":
    case "json-access": {
      return false;
    }
  }
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
export function isNeverTrue<Plan>(expression: Expression<Plan>): boolean {
  if (expression.kind !== "constant") return false;
  return expression.value === false || expression.value === null;
}

/** True for the constant TRUE. */
export function isAlwaysTrue<Plan>(expression: Expression<Plan>): boolean {
  return expression.kind === "constant" && expression.value === true;
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
  for (const node of nodesOf(expression)) {
    const isJsonColumn = node.kind === "json-access";
    const column = isJsonColumn ? replacement(node.column) : undefined;
    if (column !== undefined && column.kind !== "column") return null;
  }
  return replaceColumns(expression, replacement);
}

function replaceColumns<Plan>(
  expression: Expression<Plan>,
  replacement: Replacement<Plan>,
): Expression<Plan> {
  if (expression.kind === "json-access") {
    const column = replacement(expression.column);
    return column?.kind === "column" ? { ...expression, column } : expression;
  }
  if (expression.kind === "column") {
    return expression.depth === 0
      ? (replacement(expression) ?? expression)
      : expression;
  }
  return rebuildChildren(expression, {
    expression: (child) => replaceColumns(child, replacement),
    plan: (plan) => plan,
  });
}
