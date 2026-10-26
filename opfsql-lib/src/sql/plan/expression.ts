import type {
  AggregateFunction,
  ScalarFunction,
  StaticType,
} from "../../functions/index.js";
import type { Affinity, SqlType, Value } from "../../values/index.js";

// ---------------------------------------------------------------------------
// Bound expressions (docs/planning.md). `Plan` is the kind of plan a
// subquery holds: logical in a bound statement, physical after optimizing,
// so a physical plan never carries a logical subplan.
//
// Negation is always a `not` node around what it negates — NOT IN, NOT
// EXISTS, IS NOT NULL, NOT BETWEEN alike — so every rewrite sees one shape.
//
// Comparisons compare values as they are. The one conversion is a
// parameter's affinity, carried by the parameter node, so every comparison,
// seek and hash key agrees without converting anything itself.
// ---------------------------------------------------------------------------

/** A column: the relation that produces it, and its position there. */
export interface ColumnBinding {
  readonly tableIndex: number;
  readonly columnIndex: number;
}

/** The column index that stands for a table's rowid in a `get` or a scan. */
export { ROWID_COLUMN } from "../../schema/index.js";

/** True if two bindings name the same column of the same relation. */
export function isSameColumn(
  binding: ColumnBinding,
  other: ColumnBinding,
): boolean {
  return (
    binding.tableIndex === other.tableIndex &&
    binding.columnIndex === other.columnIndex
  );
}

/** Where `binding` is in a layout; null if it is not there. */
export function positionIn(
  layout: readonly ColumnBinding[],
  binding: ColumnBinding,
): number | null {
  const position = layout.findIndex((item) => isSameColumn(item, binding));
  return position === -1 ? null : position;
}

export type ComparisonOperator =
  "EQUAL" | "NOT_EQUAL" | "LESS" | "GREATER" | "LESS_EQUAL" | "GREATER_EQUAL";

export type ArithmeticOperator =
  "ADD" | "SUBTRACT" | "MULTIPLY" | "DIVIDE" | "MOD";

export interface ColumnReference {
  readonly kind: "column";
  readonly binding: ColumnBinding;
  /**
   * Query levels out: 0 for this query, 1 for the one enclosing it, and so
   * on. A level is a subquery in an expression, whose outer row is pushed
   * while it runs; a subquery in FROM is part of its query's plan, not a
   * level of its own.
   */
  readonly depth: number;
  /** As written, for messages and plans. */
  readonly name: string;
  readonly type: StaticType;
}

export interface Constant {
  readonly kind: "constant";
  readonly value: Value;
  readonly type: StaticType;
}

export interface Parameter {
  readonly kind: "parameter";
  /** Position in the parameters a run gets, from 0. */
  readonly index: number;
  /**
   * The conversion its value gets, from the type of what it is compared with
   * (docs/semantics.md, Comparisons), or null for none.
   */
  readonly affinity: Affinity | null;
  /** What the context expects, or ANY. */
  readonly type: StaticType;
}

export interface Comparison<Plan> {
  readonly kind: "comparison";
  readonly operator: ComparisonOperator;
  readonly left: Expression<Plan>;
  readonly right: Expression<Plan>;
  readonly type: "BOOLEAN";
}

export interface Conjunction<Plan> {
  readonly kind: "conjunction";
  readonly operator: "AND" | "OR";
  /** Two or more; nested conjunctions of the same operator are flattened. */
  readonly operands: readonly Expression<Plan>[];
  readonly type: "BOOLEAN";
}

export interface Not<Plan> {
  readonly kind: "not";
  readonly operand: Expression<Plan>;
  readonly type: "BOOLEAN";
}

/** `x IS NULL`; `x IS NOT NULL` is NOT of it. */
export interface IsNull<Plan> {
  readonly kind: "is-null";
  readonly operand: Expression<Plan>;
  readonly type: "BOOLEAN";
}

export interface Arithmetic<Plan> {
  readonly kind: "arithmetic";
  readonly operator: ArithmeticOperator;
  readonly left: Expression<Plan>;
  readonly right: Expression<Plan>;
  readonly type: StaticType;
}

export interface Negate<Plan> {
  readonly kind: "negate";
  readonly operand: Expression<Plan>;
  readonly type: StaticType;
}

/** `left || right`. */
export interface Concatenate<Plan> {
  readonly kind: "concatenate";
  readonly left: Expression<Plan>;
  readonly right: Expression<Plan>;
  readonly type: StaticType;
}

/** `needle IN (list)`; NOT IN is NOT of it. */
export interface InList<Plan> {
  readonly kind: "in-list";
  readonly needle: Expression<Plan>;
  readonly list: readonly Expression<Plan>[];
  readonly type: "BOOLEAN";
}

/** `input BETWEEN lower AND upper`: `input >= lower AND input <= upper`. */
export interface Between<Plan> {
  readonly kind: "between";
  readonly input: Expression<Plan>;
  readonly lower: Expression<Plan>;
  readonly upper: Expression<Plan>;
  readonly type: "BOOLEAN";
}

export interface FunctionCall<Plan> {
  readonly kind: "function";
  /** As written, for plans and stored index keys. */
  readonly name: string;
  readonly function: ScalarFunction;
  readonly arguments: readonly Expression<Plan>[];
  readonly type: StaticType;
}

export interface CaseBranch<Plan> {
  readonly when: Expression<Plan>;
  /** Not `then`: an object with a `then` method is a thenable. */
  readonly result: Expression<Plan>;
}

/**
 * `CASE operand WHEN value THEN …` compares the operand, evaluated once
 * per row, with each WHEN value by `=`; without an operand each WHEN is a
 * condition.
 */
export interface Case<Plan> {
  readonly kind: "case";
  readonly operand: Expression<Plan> | null;
  readonly branches: readonly CaseBranch<Plan>[];
  readonly otherwise: Expression<Plan> | null;
  readonly type: StaticType;
}

/** `CAST(operand AS type)`: its type is the type it casts to. */
export interface Cast<Plan> {
  readonly kind: "cast";
  readonly operand: Expression<Plan>;
  readonly type: SqlType;
}

export type JsonPathStep =
  | { readonly kind: "key"; readonly key: string }
  | { readonly kind: "index"; readonly index: number };

/** `column.key[0]…`: a value inside a JSON column. */
export interface JsonAccess {
  readonly kind: "json-access";
  readonly column: ColumnReference;
  readonly path: readonly JsonPathStep[];
  readonly type: StaticType;
}

/** A subquery's plan, and whether it reads columns of enclosing queries. */
interface SubqueryPart<Plan> {
  readonly plan: Plan;
  /**
   * True when it reads a column of an enclosing query, or a CTE defined
   * inside an enclosing subquery — itself or through a subquery nested in
   * it — and so runs again for each outer row. False when it can run once
   * and keep its result.
   */
  readonly isCorrelated: boolean;
}

/**
 * `(SELECT …)` as a value: its one column of its one row, or NULL without
 * rows; several rows are an error (docs/semantics.md).
 */
export interface ScalarSubquery<Plan> extends SubqueryPart<Plan> {
  readonly kind: "scalar-subquery";
  readonly type: StaticType;
}

/** `EXISTS (SELECT …)`; NOT EXISTS is NOT of it. */
export interface Exists<Plan> extends SubqueryPart<Plan> {
  readonly kind: "exists";
  readonly type: "BOOLEAN";
}

/**
 * `needle <operator> ANY | ALL (SELECT …)`; `x IN (SELECT …)` is
 * `x = ANY (…)`, and NOT IN is NOT of it.
 */
export interface QuantifiedComparison<Plan> extends SubqueryPart<Plan> {
  readonly kind: "quantified-comparison";
  readonly needle: Expression<Plan>;
  readonly operator: ComparisonOperator;
  readonly quantifier: "ANY" | "ALL";
  readonly type: "BOOLEAN";
}

export type Expression<Plan> =
  | ColumnReference
  | Constant
  | Parameter
  | Comparison<Plan>
  | Conjunction<Plan>
  | Not<Plan>
  | IsNull<Plan>
  | Arithmetic<Plan>
  | Negate<Plan>
  | Concatenate<Plan>
  | InList<Plan>
  | Between<Plan>
  | FunctionCall<Plan>
  | Case<Plan>
  | Cast<Plan>
  | JsonAccess
  | ScalarSubquery<Plan>
  | Exists<Plan>
  | QuantifiedComparison<Plan>;

/**
 * An aggregate in an aggregate node. Expressions above the node refer to
 * its result by column, never contain the call itself.
 */
export interface AggregateCall<Plan> {
  readonly name: string;
  readonly function: AggregateFunction;
  /** None for COUNT(*). */
  readonly arguments: readonly Expression<Plan>[];
  readonly isDistinct: boolean;
  readonly type: StaticType;
}

export type OrderDirection = "ascending" | "descending";

export interface OrderKey<Plan> {
  readonly expression: Expression<Plan>;
  readonly direction: OrderDirection;
  readonly nulls: "first" | "last";
}
