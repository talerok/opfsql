import { InternalError } from "../../../errors.js";
import {
  aggregateFunction,
  resolveCall,
  scalarFunction,
} from "../../../functions/index.js";
import {
  type Column,
  type Index,
  Schema,
  type Table,
} from "../../../schema/index.js";
import type {
  Affinity,
  SqlType,
  Value,
  StaticType,
} from "../../../values/index.js";
import {
  type AggregateCall,
  type ColumnReference,
  type ComparisonOperator,
  type Constant,
  formatPhysicalPlan,
  logical,
  type Parameter,
  type physical,
  ROWID_COLUMN,
  type Statement,
} from "../../plan/index.js";
import { type KeyBinder, optimize } from "../index.js";

// Small builders for logical plans, so that a test reads as the SQL it
// stands for, one line per node, and compares the plan the optimizer prints.

type Node = logical.Expression;
type Plan = logical.Plan;

// --- Schema -----------------------------------------------------------------

function columnOf(name: string, type: SqlType, isNotNull = false): Column {
  return { name, type, isNotNull, defaultValue: null };
}

/** users(id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT NOT NULL, tag ANY) */
export const users: Table = {
  name: "users",
  columns: [
    columnOf("id", "INTEGER", true),
    columnOf("name", "TEXT"),
    columnOf("age", "INTEGER"),
    columnOf("email", "TEXT", true),
    columnOf("tag", "ANY"),
  ],
  rootPage: 3,
  rowidColumn: 0,
  isAutoincrement: false,
};

/** orders(id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, status TEXT, paid BOOLEAN) */
export const orders: Table = {
  name: "orders",
  columns: [
    columnOf("id", "INTEGER", true),
    columnOf("user_id", "INTEGER"),
    columnOf("total", "REAL"),
    columnOf("status", "TEXT"),
    columnOf("paid", "BOOLEAN"),
  ],
  rootPage: 10,
  rowidColumn: 0,
  isAutoincrement: false,
};

function index(
  name: string,
  table: Table,
  keys: Index["keys"],
  isUnique = false,
): Index {
  const origin = isUnique ? "unique" : "statement";
  return { name, tableName: table.name, keys, isUnique, rootPage: 20, origin };
}

function onColumns(...positions: number[]): Index["keys"] {
  return positions.map((position) => ({ kind: "column", column: position }));
}

export const usersEmail = index("users_email", users, onColumns(3), true);
export const usersAge = index("users_age", users, onColumns(2));
export const usersNameAge = index("users_name_age", users, onColumns(1, 2));
export const usersTag = index("users_tag", users, onColumns(4));
export const usersLowerName = index("users_lower_name", users, [
  { kind: "expression", sql: "lower(name)", type: "TEXT" },
]);
export const ordersUser = index("orders_user", orders, onColumns(1));

/** events(id INTEGER PRIMARY KEY, data JSON) */
export const events: Table = {
  name: "events",
  columns: [columnOf("id", "INTEGER", true), columnOf("data", "JSON")],
  rootPage: 30,
  rowidColumn: 0,
  isAutoincrement: false,
};

export const eventsData = index("events_data", events, onColumns(1));

export const schema = new Schema(
  1,
  [users, orders, events],
  [
    usersEmail,
    usersAge,
    usersNameAge,
    usersTag,
    usersLowerName,
    ordersUser,
    eventsData,
  ],
);

/** Binds the expression keys above, as the engine's binder would. */
export const bindKey: KeyBinder = (sql, table, tableIndex) => {
  if (sql !== "lower(name)" || table.name !== "users") {
    throw new InternalError(`no key ${sql} in the test schema`);
  }
  return call("lower", column({ tableIndex, table }, "name"));
};

// --- Tables -----------------------------------------------------------------

/** A table read under `tableIndex`, whose columns print as `alias.column`. */
export interface Source {
  readonly tableIndex: number;
  readonly table: Table;
  readonly alias?: string;
}

/** `FROM table` as relation `tableIndex`: every column, or those named. */
export function get(source: Source, ...columns: string[]): logical.Get {
  const { tableIndex, table } = source;
  const names =
    columns.length === 0 ? table.columns.map(({ name }) => name) : columns;
  const positions = names.map((name) => positionOf(table, name));
  return { kind: "get", tableIndex, table, columns: positions };
}

function positionOf(table: Table, name: string): number {
  if (name === "rowid") return ROWID_COLUMN;
  const position = table.columns.findIndex((each) => each.name === name);
  if (position === -1) {
    throw new InternalError(`${table.name} has no column ${name}`);
  }
  return position;
}

/** A column of a source; `rowid` is ROWID_COLUMN. */
export function column(
  source: Source,
  name: string,
  depth = 0,
): ColumnReference {
  const columnIndex = positionOf(source.table, name);
  const type = logical.columnType(source.table, columnIndex);
  const written = source.alias === undefined ? name : `${source.alias}.${name}`;
  return {
    kind: "column",
    binding: { tableIndex: source.tableIndex, columnIndex },
    depth,
    name: written,
    type,
  };
}

/** Column `columnIndex` of a relation that is not a table: a project, an aggregate. */
export function output(
  tableIndex: number,
  columnIndex: number,
  type: StaticType,
  name = `#${String(tableIndex)}.${String(columnIndex)}`,
): ColumnReference {
  const binding = { tableIndex, columnIndex };
  return { kind: "column", binding, depth: 0, name, type };
}

// --- Expressions ------------------------------------------------------------

export function literalType(value: Value): StaticType {
  if (value === null) return "NULL";
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? "INTEGER" : "REAL";
  }
  if (typeof value === "string") return "TEXT";
  return typeof value === "boolean" ? "BOOLEAN" : "ANY";
}

export function constant(value: Value, type = literalType(value)): Constant {
  return { kind: "constant", value, type };
}

export const TRUE = constant(true);
export const FALSE = constant(false);
export const NULL = constant(null);

/** `$index + 1`, of the type its context expects, converted by `affinity`. */
export function parameter(
  position: number,
  type: StaticType = "ANY",
  affinity: Affinity | null = null,
): Parameter {
  return { kind: "parameter", index: position, affinity, type };
}

const COMPARISONS = {
  "=": "EQUAL",
  "<>": "NOT_EQUAL",
  "<": "LESS",
  "<=": "LESS_EQUAL",
  ">": "GREATER",
  ">=": "GREATER_EQUAL",
} as const satisfies Record<string, ComparisonOperator>;

export function compare(
  left: Node,
  symbol: keyof typeof COMPARISONS,
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

export function nullCheck(operand: Node): Node {
  return { kind: "is-null", operand, type: "BOOLEAN" };
}

export function inList(needle: Node, ...list: Node[]): Node {
  return { kind: "in-list", needle, list, type: "BOOLEAN" };
}

export function between(input: Node, lower: Node, upper: Node): Node {
  return { kind: "between", input, lower, upper, type: "BOOLEAN" };
}

export function plus(left: Node, right: Node): Node {
  const type =
    left.type === "INTEGER" && right.type === "INTEGER" ? "INTEGER" : "ANY";
  return { kind: "arithmetic", operator: "ADD", left, right, type };
}

export function times(left: Node, right: Node): Node {
  const type =
    left.type === "INTEGER" && right.type === "INTEGER" ? "INTEGER" : "ANY";
  return { kind: "arithmetic", operator: "MULTIPLY", left, right, type };
}

export function cast(operand: Node, type: SqlType): Node {
  return { kind: "cast", operand, type };
}

/** A call of the registry's scalar function `name`. */
export function call(name: string, ...operands: Node[]): Node {
  const definition = scalarFunction(name);
  const type = resolveCall(
    definition,
    operands.map((operand) => operand.type),
  );
  return {
    kind: "function",
    name,
    function: definition,
    arguments: operands,
    type,
  };
}

/** `random()`: volatile, never folded or moved. */
export function random(): Node {
  return call("random");
}

export function scalarSubquery(plan: Plan, isCorrelated = false): Node {
  const [type = "ANY"] = logical.typesOf(plan);
  return { kind: "scalar-subquery", plan, isCorrelated, type };
}

export function exists(plan: Plan, isCorrelated = false): Node {
  return { kind: "exists", plan, isCorrelated, type: "BOOLEAN" };
}

/** `needle IN (plan)`. */
export function inSubquery(
  needle: Node,
  plan: Plan,
  isCorrelated = false,
): Node {
  return {
    kind: "quantified-comparison",
    plan,
    isCorrelated,
    needle,
    operator: "EQUAL",
    quantifier: "ANY",
    type: "BOOLEAN",
  };
}

/** An aggregate call of the registry's `name`; no operands is `count(*)`. */
export function aggregateCall(
  name: string,
  ...operands: Node[]
): AggregateCall<Plan> {
  const definition = aggregateFunction(name);
  const types = operands.map((operand) => operand.type);
  return {
    name,
    function: definition,
    arguments: operands,
    isDistinct: false,
    type: definition.returnType(types),
  };
}

// --- Plans ------------------------------------------------------------------

export function filter(child: Plan, ...conditions: Node[]): logical.Filter {
  return { kind: "filter", child, conditions };
}

export function project(
  tableIndex: number,
  child: Plan,
  ...expressions: Node[]
): logical.Project {
  return { kind: "project", tableIndex, child, expressions };
}

export function join(
  joinType: "inner" | "left",
  left: Plan,
  right: Plan,
  ...conditions: Node[]
): logical.Join {
  return { kind: "join", joinType, left, right, conditions };
}

export function aggregate(
  indexes: { readonly groupIndex: number; readonly aggregateIndex: number },
  child: Plan,
  groups: readonly Node[],
  ...aggregates: AggregateCall<Plan>[]
): logical.Aggregate {
  return { kind: "aggregate", ...indexes, child, groups, aggregates };
}

export function asc(
  expression: Node,
  nulls: "first" | "last" = "first",
): logical.OrderKey {
  return { expression, direction: "ascending", nulls };
}

export function desc(
  expression: Node,
  nulls: "first" | "last" = "last",
): logical.OrderKey {
  return { expression, direction: "descending", nulls };
}

export function order(child: Plan, ...keys: logical.OrderKey[]): logical.Order {
  return { kind: "order", child, keys };
}

export function limit(
  child: Plan,
  count: Node | null,
  offset: Node | null = null,
): logical.Limit {
  return { kind: "limit", child, limit: count, offset };
}

export function distinct(child: Plan): logical.Distinct {
  return { kind: "distinct", child };
}

export function setOperation(
  operator: logical.SetOperation["operator"],
  tableIndex: number,
  left: Plan,
  right: Plan,
): logical.SetOperation {
  const rightTypes = logical.typesOf(right);
  const types = logical
    .typesOf(left)
    .map((type, position) => commonType(type, rightTypes[position] ?? "ANY"));
  return { kind: "set-operation", operator, tableIndex, left, right, types };
}

/** The type two sides' values share, as the binder finds it (docs/semantics.md). */
function commonType(left: StaticType, right: StaticType): StaticType {
  if (left === right) return left;
  const isNumeric = (type: StaticType): boolean =>
    type === "INTEGER" || type === "REAL";
  return isNumeric(left) && isNumeric(right) ? "REAL" : "ANY";
}

/** `WITH name AS (definition) body`, the CTE numbered `cteIndex`. */
export function cte(
  cteIndex: number,
  name: string,
  definition: Plan,
  body: Plan,
): logical.Cte {
  return { kind: "cte", cteIndex, name, definition, body };
}

/** A read of CTE `cteIndex`, of `definition`'s types, as relation `tableIndex`. */
export function cteScan(
  cteIndex: number,
  tableIndex: number,
  definition: Plan,
): logical.CteScan {
  const types = logical.typesOf(definition);
  return { kind: "cte-scan", cteIndex, tableIndex, types };
}

export function values(tableIndex: number, ...rows: Node[][]): logical.Values {
  const types = (rows[0] ?? []).map((value) => value.type);
  return { kind: "values", tableIndex, rows, types };
}

// --- Running ----------------------------------------------------------------

/** A query statement of `plan`. */
export function query(plan: Plan): Statement<Plan> {
  const columns = logical.typesOf(plan).map((type, position) => ({
    name: `c${String(position)}`,
    type,
  }));
  return { kind: "query", plan, columns, parameterCount: 0 };
}

/** The physical plan the optimizer makes of `plan`. */
export function optimizedPlan(plan: Plan): physical.Plan {
  const statement = optimize(query(plan), schema, bindKey);
  if (statement.kind !== "query") {
    throw new InternalError("a query stays a query");
  }
  return statement.plan;
}

/** The physical plan the optimizer makes of `plan`, printed. */
export function planOf(plan: Plan): string {
  return formatPhysicalPlan(optimizedPlan(plan));
}

/** Lines, as formatPhysicalPlan prints them. */
export function lines(...printed: string[]): string {
  return printed.join("\n");
}
