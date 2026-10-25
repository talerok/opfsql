import {
  resolveCall,
  type ScalarFunction,
  scalarFunction,
  type StaticType,
} from "../../functions/index.js";
import type {
  ArithmeticOperator,
  Case,
  ColumnReference,
  ComparisonOperator,
  Constant,
  Exists,
  FunctionCall,
  JsonAccess,
  JsonPathStep,
  Parameter,
  physical,
  QuantifiedComparison,
  ScalarSubquery,
} from "../../sql/plan/index.js";
import type { SqlType, Value } from "../../values/index.js";

// Small builders for expression trees, so that a test reads as the SQL it
// stands for: compare(c0, "=", parameter(0)) is `c0 = $1`.

type Node = physical.Expression;
type Plan = physical.Plan;

/** The static type the binder gives a literal of `value`. */
export function literalType(value: Value): StaticType {
  if (value === null) return "NULL";
  if (value instanceof Uint8Array) return "BLOB";
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? "INTEGER" : "REAL";
  }
  if (typeof value === "string") return "TEXT";
  return typeof value === "boolean" ? "BOOLEAN" : "JSON";
}

export function constant(
  value: Value,
  type: StaticType = literalType(value),
): Constant {
  return { kind: "constant", value, type };
}

export const NULL = constant(null);
export const TRUE = constant(true);
export const FALSE = constant(false);

export function blob(...bytes: number[]): Constant {
  return constant(new Uint8Array(bytes));
}

export function json(value: Value): Constant {
  return constant(value, "JSON");
}

export function column(
  tableIndex: number,
  columnIndex: number,
  type: StaticType = "ANY",
  depth = 0,
): ColumnReference {
  const binding = { tableIndex, columnIndex };
  const name = `t${String(tableIndex)}.c${String(columnIndex)}`;
  return { kind: "column", binding, depth, name, type };
}

/** A column of the query `depth` levels out. */
export function outer(
  depth: number,
  tableIndex: number,
  columnIndex: number,
  type: StaticType = "INTEGER",
): ColumnReference {
  return column(tableIndex, columnIndex, type, depth);
}

export function parameter(index: number, type: StaticType = "ANY"): Parameter {
  return { kind: "parameter", index, type };
}

export type ComparisonSymbol = "=" | "<>" | "<" | "<=" | ">" | ">=";

const COMPARISONS: Record<ComparisonSymbol, ComparisonOperator> = {
  "=": "EQUAL",
  "<>": "NOT_EQUAL",
  "<": "LESS",
  "<=": "LESS_EQUAL",
  ">": "GREATER",
  ">=": "GREATER_EQUAL",
};

export function compare(
  left: Node,
  symbol: ComparisonSymbol,
  right: Node,
): Node {
  const operator = COMPARISONS[symbol];
  return { kind: "comparison", operator, left, right, type: "BOOLEAN" };
}

export function and(...operands: Node[]): Node {
  return { kind: "conjunction", operator: "AND", operands, type: "BOOLEAN" };
}

export function or(...operands: Node[]): Node {
  return { kind: "conjunction", operator: "OR", operands, type: "BOOLEAN" };
}

export function not(operand: Node): Node {
  return { kind: "not", operand, type: "BOOLEAN" };
}

/** `operand IS NULL`. */
export function nullCheck(operand: Node): Node {
  return { kind: "is-null", operand, isNegated: false, type: "BOOLEAN" };
}

/** `operand IS NOT NULL`. */
export function notNullCheck(operand: Node): Node {
  return { kind: "is-null", operand, isNegated: true, type: "BOOLEAN" };
}

type ArithmeticSymbol = "+" | "-" | "*" | "/" | "%";

const ARITHMETIC: Record<ArithmeticSymbol, ArithmeticOperator> = {
  "+": "ADD",
  "-": "SUBTRACT",
  "*": "MULTIPLY",
  "/": "DIVIDE",
  "%": "MOD",
};

/** `left <symbol> right`, typed as the binder types arithmetic. */
export function arithmetic(
  left: Node,
  symbol: ArithmeticSymbol,
  right: Node,
): Node {
  const operator = ARITHMETIC[symbol];
  const type = arithmeticType(left.type, right.type);
  return { kind: "arithmetic", operator, left, right, type };
}

function arithmeticType(left: StaticType, right: StaticType): StaticType {
  if (left === "INTEGER" && right === "INTEGER") return "INTEGER";
  return left === "REAL" || right === "REAL" ? "REAL" : "ANY";
}

export function negate(operand: Node): Node {
  return { kind: "negate", operand, type: operand.type };
}

export function concat(left: Node, right: Node): Node {
  return { kind: "concatenate", left, right, type: "TEXT" };
}

export function inList(needle: Node, ...list: Node[]): Node {
  return { kind: "in-list", needle, list, isNegated: false, type: "BOOLEAN" };
}

export function notInList(needle: Node, ...list: Node[]): Node {
  return { kind: "in-list", needle, list, isNegated: true, type: "BOOLEAN" };
}

export function between(input: Node, lower: Node, upper: Node): Node {
  return { kind: "between", input, lower, upper, type: "BOOLEAN" };
}

/** A call of the registry's function `name`, typed as the binder types it. */
export function call(name: string, ...operands: Node[]): FunctionCall<Plan> {
  return callOf(scalarFunction(name), operands);
}

/** A call of `definition`, which need not be in the registry. */
export function callOf(
  definition: ScalarFunction,
  operands: readonly Node[],
): FunctionCall<Plan> {
  const type = resolveCall(
    definition,
    operands.map((operand) => operand.type),
  );
  return {
    kind: "function",
    name: definition.name,
    function: definition,
    arguments: operands,
    type,
  };
}

/** `CASE WHEN … THEN … ELSE … END`. */
export function caseWhen(
  branches: readonly [Node, Node][],
  otherwise: Node | null = null,
): Case<Plan> {
  return simpleCase(null, branches, otherwise);
}

/** `CASE operand WHEN … THEN … ELSE … END`. */
export function simpleCase(
  operand: Node | null,
  branches: readonly [Node, Node][],
  otherwise: Node | null = null,
): Case<Plan> {
  return {
    kind: "case",
    operand,
    branches: branches.map(([when, result]) => ({ when, result })),
    otherwise,
    type: "ANY",
  };
}

export function cast(operand: Node, target: SqlType): Node {
  return { kind: "cast", operand, target, type: target };
}

/** `column.key[index]…`: strings are keys, numbers indexes. */
export function jsonAccess(
  target: ColumnReference,
  ...path: (string | number)[]
): JsonAccess {
  const steps = path.map((step) => jsonStep(step));
  return { kind: "json-access", column: target, path: steps, type: "JSON" };
}

function jsonStep(step: string | number): JsonPathStep {
  if (typeof step === "string") return { kind: "key", key: step };
  return { kind: "index", index: step };
}

// Plans, for the fake executor (harness.ts) to run as subqueries.

/** `VALUES (…), (…)` as table `tableIndex`, typed by its first row. */
export function values(tableIndex: number, ...rows: Node[][]): physical.Values {
  const types = (rows[0] ?? []).map((value) => value.type);
  return { kind: "values", tableIndex, rows, types };
}

/** `SELECT expressions FROM child` as table `tableIndex`. */
export function project(
  tableIndex: number,
  child: Plan,
  ...expressions: Node[]
): physical.Project {
  const names = expressions.map((_expression, index) => `c${String(index)}`);
  return { kind: "project", tableIndex, child, expressions, names };
}

/** `child WHERE conditions`. */
export function filter(child: Plan, ...conditions: Node[]): physical.Filter {
  return { kind: "filter", child, conditions };
}

/** Whether a subquery reads a column of an enclosing query. */
export type Correlation = "correlated" | "uncorrelated";

export function scalarSubquery(
  plan: Plan,
  correlation: Correlation = "uncorrelated",
): ScalarSubquery<Plan> {
  const isCorrelated = correlation === "correlated";
  const type = firstColumnType(plan);
  return { kind: "scalar-subquery", plan, isCorrelated, type };
}

export function exists(
  plan: Plan,
  correlation: Correlation = "uncorrelated",
): Exists<Plan> {
  const isCorrelated = correlation === "correlated";
  return {
    kind: "exists",
    plan,
    isCorrelated,
    isNegated: false,
    type: "BOOLEAN",
  };
}

export function notExists(
  plan: Plan,
  correlation: Correlation = "uncorrelated",
): Exists<Plan> {
  return { ...exists(plan, correlation), isNegated: true };
}

/** `needle <symbol> ANY (plan)`. */
export function compareAny(
  needle: Node,
  symbol: ComparisonSymbol,
  plan: Plan,
  correlation: Correlation = "uncorrelated",
): QuantifiedComparison<Plan> {
  return {
    ...compareAll(needle, symbol, plan, correlation),
    quantifier: "ANY",
  };
}

/** `needle <symbol> ALL (plan)`. */
export function compareAll(
  needle: Node,
  symbol: ComparisonSymbol,
  plan: Plan,
  correlation: Correlation = "uncorrelated",
): QuantifiedComparison<Plan> {
  return {
    kind: "quantified-comparison",
    plan,
    isCorrelated: correlation === "correlated",
    needle,
    operator: COMPARISONS[symbol],
    quantifier: "ALL",
    columnType: firstColumnType(plan),
    type: "BOOLEAN",
  };
}

/** `needle IN (plan)`: `needle = ANY (plan)`. */
export function inSubquery(
  needle: Node,
  plan: Plan,
  correlation: Correlation = "uncorrelated",
): QuantifiedComparison<Plan> {
  return compareAny(needle, "=", plan, correlation);
}

function firstColumnType(plan: Plan): StaticType {
  if (plan.kind === "values") return plan.types[0] ?? "ANY";
  if (plan.kind === "project") return plan.expressions[0]?.type ?? "ANY";
  return plan.kind === "filter" ? firstColumnType(plan.child) : "ANY";
}
