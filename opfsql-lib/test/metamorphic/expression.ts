import type { Value } from "../../src/values/index.js";

// ---------------------------------------------------------------------------
// Generated expressions: SQL text with its static type, kept as a tree so a
// failing condition can be reduced to one of its parts, and its constants
// turned into parameters.
// ---------------------------------------------------------------------------

/**
 * The static type of an expression, as the binder sees it. `untyped` has
 * none — an ANY column, a JSON value — and compares with anything, but
 * goes into no arithmetic, function or condition, where a value of the
 * wrong kind would fail at run time.
 */
export type ExpressionType =
  "integer" | "real" | "text" | "boolean" | "untyped";

/** SQL that is one value: a column, a constant, a subquery. */
export interface Leaf {
  readonly kind: "leaf";
  readonly type: ExpressionType;
  readonly sql: string;
}

/** A constant, with the value a parameter standing in for it takes. */
export interface Constant {
  readonly kind: "constant";
  readonly type: ExpressionType;
  readonly sql: string;
  readonly value: Value;
}

/** SQL fragments around child expressions, printed in parentheses. */
export interface Node {
  readonly kind: "node";
  readonly type: ExpressionType;
  readonly parts: readonly (string | Expression)[];
}

export type Expression = Leaf | Constant | Node;

export function leaf(type: ExpressionType, sql: string): Leaf {
  return { kind: "leaf", type, sql };
}

/** A constant written as `sql`: NULL, TRUE, FALSE, a number or quoted text. */
export function constant(type: ExpressionType, sql: string): Constant {
  return { kind: "constant", type, sql, value: literalValue(sql) };
}

function literalValue(sql: string): Value {
  if (sql === "NULL") return null;
  if (sql === "TRUE") return true;
  if (sql === "FALSE") return false;
  if (sql.startsWith("'")) return sql.slice(1, -1).replaceAll("''", "'");
  return Number(sql);
}

/** `node("boolean", left, " = ", right)` prints `(left = right)`. */
export function node(
  type: ExpressionType,
  ...parts: (string | Expression)[]
): Node {
  return { kind: "node", type, parts };
}

export function print(expression: Expression): string {
  if (expression.kind !== "node") return expression.sql;
  const text = expression.parts.map((part) =>
    typeof part === "string" ? part : print(part),
  );
  return `(${text.join("")})`;
}

export function isNumeric(type: ExpressionType): boolean {
  return type === "integer" || type === "real";
}

/** Whether `=` and `<` accept the two: numbers together, a type with itself, untyped with anything. */
export function areComparable(a: ExpressionType, b: ExpressionType): boolean {
  if (a === "untyped" || b === "untyped") return true;
  return a === b || (isNumeric(a) && isNumeric(b));
}

/**
 * Smaller expressions that could stand in for `expression`: its children of the
 * same type, then NULL. A reducer tries them in turn.
 */
export function replacements(expression: Expression): Expression[] {
  if (expression.kind !== "node") return [];
  const children = expression.parts.filter(
    (part): part is Expression =>
      typeof part !== "string" && part.type === expression.type,
  );
  return [...children, leaf(expression.type, "NULL")];
}

/** `expression` with its child at `index` (among its expression parts) replaced. */
export function withChild(
  expression: Node,
  index: number,
  child: Expression,
): Node {
  let seen = -1;
  const parts = expression.parts.map((part) => {
    if (typeof part === "string") return part;
    seen++;
    return seen === index ? child : part;
  });
  return { ...expression, parts };
}

export function childrenOf(expression: Expression): Expression[] {
  if (expression.kind !== "node") return [];
  return expression.parts.filter(
    (part): part is Expression => typeof part !== "string",
  );
}

/**
 * `expression` with the constants `isChosen` picks as parameters, numbered after
 * those already in `parameters`, to which their values are added.
 */
export function parameterized(
  expression: Expression,
  isChosen: () => boolean,
  parameters: Value[],
): Expression {
  if (expression.kind === "leaf") return expression;
  if (expression.kind === "constant") {
    if (!isChosen()) return expression;
    parameters.push(expression.value);
    return leaf(expression.type, `$${String(parameters.length)}`);
  }
  const parts = expression.parts.map((part) =>
    typeof part === "string" ? part : parameterized(part, isChosen, parameters),
  );
  return { ...expression, parts };
}
