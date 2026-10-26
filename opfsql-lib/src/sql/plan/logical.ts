import type { StaticType } from "../../functions/index.js";
import type { Table } from "../../schema/index.js";
import type {
  AggregateCall,
  ColumnBinding,
  Expression as GenericExpression,
  OrderKey as GenericOrderKey,
} from "./expression.js";
import { ROWID_COLUMN } from "./expression.js";

// ---------------------------------------------------------------------------
// Logical plans (docs/planning.md, Logical plans): what to compute, never
// how. No node mentions an index, an estimate or an algorithm.
// ---------------------------------------------------------------------------

export type Expression = GenericExpression<Plan>;
export type OrderKey = GenericOrderKey<Plan>;

/** The rows of a table. */
export interface Get {
  readonly kind: "get";
  readonly tableIndex: number;
  readonly table: Table;
  /** Positions of the columns read, in output order; ROWID_COLUMN is the rowid. */
  readonly columns: readonly number[];
}

/** Literal rows; a SELECT without FROM is one row of no columns. */
export interface Values {
  readonly kind: "values";
  readonly tableIndex: number;
  readonly rows: readonly (readonly Expression[])[];
  readonly types: readonly StaticType[];
}

/** The rows of its child for which every condition is TRUE. */
export interface Filter {
  readonly kind: "filter";
  readonly child: Plan;
  /** The conjuncts of the condition; never empty. */
  readonly conditions: readonly Expression[];
}

/** Output names live in the statement (Query.columns), in one place. */
export interface Project {
  readonly kind: "project";
  readonly tableIndex: number;
  readonly child: Plan;
  readonly expressions: readonly Expression[];
}

/**
 * Groups of its child's rows and the aggregates over each. Its output is the
 * group keys, bound to `groupIndex`, then the aggregates, bound to
 * `aggregateIndex`. No groups means one group of every row, even of none.
 */
export interface Aggregate {
  readonly kind: "aggregate";
  readonly groupIndex: number;
  readonly aggregateIndex: number;
  readonly child: Plan;
  readonly groups: readonly Expression[];
  readonly aggregates: readonly AggregateCall<Plan>[];
}

/**
 * Pairs of rows for which every condition is TRUE; no conditions means every
 * pair (a cross join). A LEFT join also emits each left row that pairs with
 * none, with NULLs.
 */
export interface Join {
  readonly kind: "join";
  readonly joinType: "inner" | "left";
  readonly left: Plan;
  readonly right: Plan;
  /** The conjuncts of the ON condition, as in a filter. */
  readonly conditions: readonly Expression[];
}

export interface Order {
  readonly kind: "order";
  readonly child: Plan;
  readonly keys: readonly OrderKey[];
}

/** At most `limit` rows after skipping `offset`; each a constant or parameter. */
export interface Limit {
  readonly kind: "limit";
  readonly child: Plan;
  /** Null: no limit. */
  readonly limit: Expression | null;
  /** Null: none skipped. */
  readonly offset: Expression | null;
}

export interface Distinct {
  readonly kind: "distinct";
  readonly child: Plan;
}

/** Its own table index binds the result's columns, so ORDER BY can name them. */
export interface SetOperation {
  readonly kind: "set-operation";
  readonly operator: "UNION" | "UNION ALL" | "INTERSECT" | "EXCEPT";
  readonly tableIndex: number;
  readonly left: Plan;
  readonly right: Plan;
  /** The common types of the two sides' columns. */
  readonly types: readonly StaticType[];
}

/**
 * A materialized subplan, read by `cte-scan`s of its index within `body`. A
 * recursive CTE is a `cte` whose definition is a `recursive-cte`: the body
 * reads the full result through this index.
 */
export interface Cte {
  readonly kind: "cte";
  readonly cteIndex: number;
  readonly name: string;
  readonly definition: Plan;
  readonly body: Plan;
}

export interface CteScan {
  readonly kind: "cte-scan";
  readonly cteIndex: number;
  readonly tableIndex: number;
  readonly types: readonly StaticType[];
}

/**
 * `anchor`, then `step` run again and again on the rows the last run added
 * until it adds none. `cteIndex` names that working set: `step` reads it
 * through a `cte-scan` of this index, never the full result.
 */
export interface RecursiveCte {
  readonly kind: "recursive-cte";
  readonly cteIndex: number;
  readonly name: string;
  readonly tableIndex: number;
  readonly anchor: Plan;
  readonly step: Plan;
  /** UNION ALL keeps duplicates; UNION adds only rows not seen yet. */
  readonly isUnionAll: boolean;
  readonly types: readonly StaticType[];
}

export type Plan =
  | Get
  | Values
  | Filter
  | Project
  | Aggregate
  | Join
  | Order
  | Limit
  | Distinct
  | SetOperation
  | Cte
  | CteScan
  | RecursiveCte;

/** The bindings of a node's output columns, in order. */
export function layoutOf(plan: Plan): ColumnBinding[] {
  switch (plan.kind) {
    case "get": {
      return plan.columns.map((columnIndex) => ({
        tableIndex: plan.tableIndex,
        columnIndex,
      }));
    }
    case "values": {
      return positions(plan.tableIndex, plan.types.length);
    }
    case "project": {
      return positions(plan.tableIndex, plan.expressions.length);
    }
    case "aggregate": {
      return [
        ...positions(plan.groupIndex, plan.groups.length),
        ...positions(plan.aggregateIndex, plan.aggregates.length),
      ];
    }
    case "join": {
      return [...layoutOf(plan.left), ...layoutOf(plan.right)];
    }
    case "filter":
    case "order":
    case "limit":
    case "distinct": {
      return layoutOf(plan.child);
    }
    case "set-operation": {
      return positions(plan.tableIndex, layoutOf(plan.left).length);
    }
    case "cte": {
      return layoutOf(plan.body);
    }
    case "cte-scan": {
      return positions(plan.tableIndex, plan.types.length);
    }
    case "recursive-cte": {
      return positions(plan.tableIndex, layoutOf(plan.anchor).length);
    }
  }
}

/** The static types of a node's output columns, in layout order. */
export function typesOf(plan: Plan): StaticType[] {
  switch (plan.kind) {
    case "get": {
      return plan.columns.map((column) => columnType(plan.table, column));
    }
    case "project": {
      return plan.expressions.map((expression) => expression.type);
    }
    case "aggregate": {
      return [
        ...plan.groups.map((group) => group.type),
        ...plan.aggregates.map((call) => call.type),
      ];
    }
    case "join": {
      return [...typesOf(plan.left), ...typesOf(plan.right)];
    }
    case "filter":
    case "order":
    case "limit":
    case "distinct": {
      return typesOf(plan.child);
    }
    case "cte": {
      return typesOf(plan.body);
    }
    case "values":
    case "set-operation":
    case "cte-scan":
    case "recursive-cte": {
      return [...plan.types];
    }
  }
}

/** A table column's type; the rowid's is INTEGER. */
export function columnType(table: Table, column: number): StaticType {
  if (column === ROWID_COLUMN) return "INTEGER";
  return table.columns[column]?.type ?? "ANY";
}

/** Columns 0 to count − 1 of one relation. */
export function positions(tableIndex: number, count: number): ColumnBinding[] {
  return Array.from({ length: count }, (_, columnIndex) => ({
    tableIndex,
    columnIndex,
  }));
}
