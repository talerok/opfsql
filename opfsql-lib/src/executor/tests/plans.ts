import {
  aggregateFunction,
  resolveCall,
  type ScalarFunction,
  type StaticType,
} from "../../functions/index.js";
import { callOf } from "../../expressions/tests/builders.js";
import type { Index, Table } from "../../schema/index.js";
import {
  type AggregateCall,
  type Assignment,
  type Delete,
  type Insert,
  type OnConflict,
  type OrderDirection,
  physical,
  type Update,
} from "../../sql/plan/index.js";

// Small builders for physical plans, so that a test reads as the plan it
// stands for, one line per node. Expressions come from the expression
// compiler's own test builders.

export {
  and,
  arithmetic,
  call,
  callOf,
  column,
  compare,
  constant,
  exists,
  inSubquery,
  not,
  notExists,
  NULL,
  or,
  outer,
  parameter,
  scalarSubquery,
  values,
} from "../../expressions/tests/builders.js";

type Plan = physical.Plan;
type Node = physical.Expression;

/** What a scan reads: a table under its table index, and the columns it outputs. */
export interface Source {
  readonly table: Table;
  readonly tableIndex: number;
  readonly columns: number[];
}

interface ScanOptions {
  readonly residual?: Node[];
  readonly direction?: OrderDirection;
}

/** The aggregate index an index-extreme's one column is bound to here. */
export const EXTREME_INDEX = 90;

export function source(
  table: Table,
  tableIndex: number,
  columns: number[],
): Source {
  return { table, tableIndex, columns };
}

export function tableScan(
  from: Source,
  { residual = [], direction = "ascending" }: ScanOptions = {},
): physical.TableScan {
  return { kind: "table-scan", ...from, residual, direction };
}

export function rowidList(
  from: Source,
  rowids: Node[],
  { residual = [], direction = "ascending" }: ScanOptions = {},
): physical.RowidLookup {
  const set = { kind: "list", values: rowids } as const;
  return { kind: "rowid-lookup", ...from, residual, rowids: set, direction };
}

export function rowidRange(
  from: Source,
  bounds: physical.Range,
  { residual = [], direction = "ascending" }: ScanOptions = {},
): physical.RowidLookup {
  const set = { kind: "range", range: bounds } as const;
  return { kind: "rowid-lookup", ...from, residual, rowids: set, direction };
}

export function indexScan(
  from: Source,
  index: Index,
  keyRange: physical.KeyRange,
  { residual = [], direction = "ascending" }: ScanOptions = {},
): physical.IndexScan {
  return {
    kind: "index-scan",
    ...from,
    residual,
    index,
    keys: keyRange,
    direction,
  };
}

export function indexUnion(
  from: Source,
  branches: physical.IndexBranch[],
  residual: Node[] = [],
): physical.IndexUnion {
  return { kind: "index-union", ...from, residual, branches };
}

/** MIN or MAX of `index`'s part after `equal`, bound to EXTREME_INDEX. */
export function indexExtreme(
  table: Table,
  index: Index,
  extreme: "MIN" | "MAX",
  equal: Node[] = [],
): physical.IndexExtreme {
  return {
    kind: "index-extreme",
    aggregateIndex: EXTREME_INDEX,
    table,
    index,
    equal,
    extreme,
    type: "ANY",
  };
}

/** Entries whose first parts equal `equal`, the next maybe in `range`. */
export function keys(
  equal: Node[],
  range: physical.Range | null = null,
): physical.KeyRange {
  return { equal, range };
}

export function between(
  lower: physical.Bound | null,
  upper: physical.Bound | null,
): physical.Range {
  return { lower, upper };
}

/** An end of a range that includes `value`. */
export function including(value: Node): physical.Bound {
  return { value, isInclusive: true };
}

/** An end of a range that excludes `value`. */
export function excluding(value: Node): physical.Bound {
  return { value, isInclusive: false };
}

export function filter(child: Plan, ...conditions: Node[]): physical.Filter {
  return { kind: "filter", child, conditions };
}

export function project(
  tableIndex: number,
  child: Plan,
  ...expressions: Node[]
): physical.Project {
  return { kind: "project", tableIndex, child, expressions };
}

interface JoinOptions {
  readonly residual?: Node[];
  readonly joinType?: "inner" | "left";
}

export function hashJoin(
  [left, right]: [Plan, Plan],
  [leftKeys, rightKeys]: [Node[], Node[]],
  { residual = [], joinType = "inner" }: JoinOptions = {},
): physical.HashJoin {
  return {
    kind: "hash-join",
    joinType,
    left,
    right,
    leftKeys,
    rightKeys,
    residual,
  };
}

export function nestedLoopJoin(
  left: Plan,
  right: Plan,
  conditions: Node[] = [],
  joinType: "inner" | "left" = "inner",
): physical.NestedLoopJoin {
  return { kind: "nested-loop-join", joinType, left, right, conditions };
}

export function lookupJoin(
  left: Plan,
  right: physical.RowidLookup | physical.IndexScan,
  { residual = [], joinType = "inner" }: JoinOptions = {},
): physical.LookupJoin {
  return { kind: "lookup-join", joinType, left, right, residual };
}

export function hashAggregate(
  [groupIndex, aggregateIndex]: [number, number],
  child: Plan,
  groups: Node[],
  aggregates: AggregateCall<Plan>[],
): physical.HashAggregate {
  return {
    kind: "hash-aggregate",
    groupIndex,
    aggregateIndex,
    child,
    groups,
    aggregates,
  };
}

/** `name(arguments)`, or `name(DISTINCT arguments)`; COUNT(*) has no arguments. */
export function aggregateCall(
  name: string,
  operands: Node[],
  isDistinct = false,
): AggregateCall<Plan> {
  const definition = aggregateFunction(name);
  const types = operands.map((operand) => operand.type);
  const type = resolveCall(definition, types);
  return { name, function: definition, arguments: operands, isDistinct, type };
}

export function sort(
  child: Plan,
  ...orderKeys: physical.OrderKey[]
): physical.Sort {
  return { kind: "sort", child, keys: orderKeys };
}

export function ascending(
  expression: Node,
  nulls: "first" | "last" = "first",
): physical.OrderKey {
  return { expression, direction: "ascending", nulls };
}

export function descending(
  expression: Node,
  nulls: "first" | "last" = "last",
): physical.OrderKey {
  return { expression, direction: "descending", nulls };
}

export function topN(
  child: Plan,
  orderKeys: physical.OrderKey[],
  count: Node,
  offset: Node | null = null,
): physical.TopN {
  return { kind: "top-n", child, keys: orderKeys, limit: count, offset };
}

export function limit(
  child: Plan,
  count: Node | null,
  offset: Node | null = null,
): physical.Limit {
  return { kind: "limit", child, limit: count, offset };
}

export function distinct(child: Plan): physical.HashDistinct {
  return { kind: "hash-distinct", child };
}

export function setOperation(
  operator: physical.SetOperation["operator"],
  tableIndex: number,
  left: Plan,
  right: Plan,
): physical.SetOperation {
  const types = physical.typesOf(left);
  return { kind: "set-operation", operator, tableIndex, left, right, types };
}

export function cte(
  cteIndex: number,
  definition: Plan,
  body: Plan,
): physical.Cte {
  return {
    kind: "cte",
    cteIndex,
    name: `cte${String(cteIndex)}`,
    definition,
    body,
  };
}

export function cteScan(
  cteIndex: number,
  tableIndex: number,
  types: StaticType[] = ["ANY"],
): physical.CteScan {
  return { kind: "cte-scan", cteIndex, tableIndex, types };
}

/** A recursive CTE whose working set is `cteIndex`, its result bound to `tableIndex`. */
export function recursiveCte(
  [cteIndex, tableIndex]: [number, number],
  anchor: Plan,
  step: Plan,
  isUnionAll: boolean,
): physical.RecursiveCte {
  return {
    kind: "recursive-cte",
    cteIndex,
    name: `recursive${String(cteIndex)}`,
    tableIndex,
    anchor,
    step,
    isUnionAll,
    types: physical.typesOf(anchor),
  };
}

/**
 * A function that returns its argument and counts how often it is compiled
 * (implemented, once per compile of its expression) and called (per row).
 */
export class Counting {
  compiles = 0;
  calls = 0;

  readonly definition: ScalarFunction = {
    kind: "scalar",
    name: "counting",
    signature: { required: ["any"] },
    returnType: ([type = "ANY"]) => type,
    implement: () => {
      this.compiles++;
      return {
        kind: "eager",
        call: ([value = null]) => {
          this.calls++;
          return value;
        },
      };
    },
  };

  of(operand: Node): Node {
    return callOf(this.definition, [operand]);
  }
}

/** What an INSERT does besides filling columns from a source. */
interface InsertOptions {
  /** True when the source reads the target table, as the binder says. */
  readonly readsTarget?: boolean;
  readonly onConflict?: OnConflict<Plan> | null;
}

export function insert(
  table: Table,
  columns: number[],
  from: Plan,
  { readsTarget = false, onConflict = null }: InsertOptions = {},
): Insert<Plan> {
  return {
    kind: "insert",
    table,
    columns,
    source: from,
    readsTarget,
    onConflict,
    parameterCount: 0,
  };
}

/** What an UPDATE does besides its assignments. */
interface UpdateOptions {
  /** True when an assignment's subquery reads the target table, as the binder says. */
  readonly readsTarget?: boolean;
}

export function update(
  from: Source,
  sourcePlan: Plan,
  assignments: Assignment<Plan>[],
  { readsTarget = false }: UpdateOptions = {},
): Update<Plan> {
  return {
    kind: "update",
    table: from.table,
    tableIndex: from.tableIndex,
    source: sourcePlan,
    assignments,
    readsTarget,
    parameterCount: 0,
  };
}

export function deleteFrom(from: Source, sourcePlan: Plan): Delete<Plan> {
  return {
    kind: "delete",
    table: from.table,
    tableIndex: from.tableIndex,
    source: sourcePlan,
    parameterCount: 0,
  };
}
