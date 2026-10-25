import type { StaticType } from "../../functions/index.js";
import type { Index, Table } from "../../schema/index.js";
import type {
  AggregateCall,
  ColumnBinding,
  Expression as GenericExpression,
  OrderDirection,
  OrderKey as GenericOrderKey,
} from "./expression.js";
import { positions } from "./logical.js";

// ---------------------------------------------------------------------------
// Physical plans (docs/planning.md, Physical plans): access paths and
// algorithms, chosen by the optimizer. Every scan carries `residual`, the
// conditions checked on each row it returns: an access path only narrows
// what is read, it never stands in for a condition.
// ---------------------------------------------------------------------------

export type Expression = GenericExpression<Plan>;
export type OrderKey = GenericOrderKey<Plan>;

/** One end of a range: a constant or parameter, and whether it is included. */
export interface Bound {
  readonly value: Expression;
  readonly isInclusive: boolean;
}

/** Null at an end: unbounded there. */
export interface Range {
  readonly lower: Bound | null;
  readonly upper: Bound | null;
}

/**
 * Index entries whose first key parts equal `equal`, and whose next part,
 * if a range is given, lies in it.
 */
export interface KeyRange {
  readonly equal: readonly Expression[];
  readonly range: Range | null;
}

/** What every scan of a table has: which table, what it outputs, what it checks. */
interface TableAccess {
  readonly tableIndex: number;
  readonly table: Table;
  /** As in a logical `get`; ROWID_COLUMN is the rowid. */
  readonly columns: readonly number[];
  readonly residual: readonly Expression[];
}

export interface TableScan extends TableAccess {
  readonly kind: "table-scan";
  /** Rowid order; descending reads from the largest. */
  readonly direction: OrderDirection;
}

export interface RowidLookup extends TableAccess {
  readonly kind: "rowid-lookup";
  readonly rowids:
    | { readonly kind: "list"; readonly values: readonly Expression[] }
    | { readonly kind: "range"; readonly range: Range };
}

export interface IndexScan extends TableAccess {
  readonly kind: "index-scan";
  readonly index: Index;
  readonly keys: KeyRange;
  /** Key order; descending reads from the largest key. */
  readonly direction: OrderDirection;
}

/** The rows of several scans of one table, each row once: an OR of indexable conditions. */
export interface IndexUnion extends TableAccess {
  readonly kind: "index-union";
  readonly branches: readonly (IndexScan | RowidLookup)[];
}

/**
 * MIN or MAX of an index's next key part after `equal`, read from one end of
 * the index, NULLs skipped: the one row of an aggregate with that one call.
 */
export interface IndexExtreme {
  readonly kind: "index-extreme";
  readonly aggregateIndex: number;
  readonly table: Table;
  readonly index: Index;
  readonly equal: readonly Expression[];
  readonly extreme: "MIN" | "MAX";
  readonly type: StaticType;
}

export interface Filter {
  readonly kind: "filter";
  readonly child: Plan;
  readonly conditions: readonly Expression[];
}

export interface Project {
  readonly kind: "project";
  readonly tableIndex: number;
  readonly child: Plan;
  readonly expressions: readonly Expression[];
  readonly names: readonly string[];
}

/**
 * Rows of `right` hashed by `rightKeys`; each left row meets those whose keys
 * equal its `leftKeys` (by valueKey, NULL keys never), and a pair is a match
 * when `residual` is TRUE for it, or there is none.
 */
export interface HashJoin {
  readonly kind: "hash-join";
  readonly joinType: "inner" | "left";
  readonly left: Plan;
  readonly right: Plan;
  readonly leftKeys: readonly Expression[];
  readonly rightKeys: readonly Expression[];
  readonly residual: Expression | null;
}

export interface NestedLoopJoin {
  readonly kind: "nested-loop-join";
  readonly joinType: "inner" | "left";
  readonly left: Plan;
  readonly right: Plan;
  readonly condition: Expression | null;
}

export interface HashAggregate {
  readonly kind: "hash-aggregate";
  readonly groupIndex: number;
  readonly aggregateIndex: number;
  readonly child: Plan;
  readonly groups: readonly Expression[];
  readonly aggregates: readonly AggregateCall<Plan>[];
}

export interface Sort {
  readonly kind: "sort";
  readonly child: Plan;
  readonly keys: readonly OrderKey[];
}

/** A sort and the limit above it: the first `limit` rows after `offset`. */
export interface TopN {
  readonly kind: "top-n";
  readonly child: Plan;
  readonly keys: readonly OrderKey[];
  readonly limit: Expression;
  readonly offset: Expression | null;
}

export interface Limit {
  readonly kind: "limit";
  readonly child: Plan;
  readonly limit: Expression | null;
  readonly offset: Expression | null;
}

export interface HashDistinct {
  readonly kind: "hash-distinct";
  readonly child: Plan;
}

export interface SetOperation {
  readonly kind: "set-operation";
  readonly operator: "UNION" | "UNION ALL" | "INTERSECT" | "EXCEPT";
  readonly tableIndex: number;
  readonly left: Plan;
  readonly right: Plan;
}

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

export interface RecursiveCte {
  readonly kind: "recursive-cte";
  readonly cteIndex: number;
  readonly name: string;
  readonly tableIndex: number;
  readonly anchor: Plan;
  readonly step: Plan;
  readonly isUnionAll: boolean;
}

export interface Values {
  readonly kind: "values";
  readonly tableIndex: number;
  readonly rows: readonly (readonly Expression[])[];
  readonly types: readonly StaticType[];
}

export type Plan =
  | TableScan
  | RowidLookup
  | IndexScan
  | IndexUnion
  | IndexExtreme
  | Filter
  | Project
  | HashJoin
  | NestedLoopJoin
  | HashAggregate
  | Sort
  | TopN
  | Limit
  | HashDistinct
  | SetOperation
  | Cte
  | CteScan
  | RecursiveCte
  | Values;

/** The bindings of a node's output columns, in order. */
export function layoutOf(plan: Plan): ColumnBinding[] {
  switch (plan.kind) {
    case "table-scan":
    case "rowid-lookup":
    case "index-scan":
    case "index-union": {
      return plan.columns.map((columnIndex) => ({
        tableIndex: plan.tableIndex,
        columnIndex,
      }));
    }
    case "index-extreme": {
      return positions(plan.aggregateIndex, 1);
    }
    case "project": {
      return positions(plan.tableIndex, plan.expressions.length);
    }
    case "hash-join":
    case "nested-loop-join": {
      return [...layoutOf(plan.left), ...layoutOf(plan.right)];
    }
    case "hash-aggregate": {
      return [
        ...positions(plan.groupIndex, plan.groups.length),
        ...positions(plan.aggregateIndex, plan.aggregates.length),
      ];
    }
    case "filter":
    case "sort":
    case "top-n":
    case "limit":
    case "hash-distinct": {
      return layoutOf(plan.child);
    }
    case "set-operation": {
      return positions(plan.tableIndex, layoutOf(plan.left).length);
    }
    case "cte": {
      return layoutOf(plan.body);
    }
    case "cte-scan":
    case "values": {
      return positions(plan.tableIndex, plan.types.length);
    }
    case "recursive-cte": {
      return positions(plan.tableIndex, layoutOf(plan.anchor).length);
    }
  }
}
