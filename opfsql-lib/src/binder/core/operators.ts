import type { TableSchema } from "../../store/types.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";
import type { BindContext } from "./context.js";
import type { BindingEntry } from "./scope.js";

export function makeEmptyGet(ctx: BindContext): BT.LogicalGet {
  const tableIndex = ctx.nextTableIndex();
  const emptySchema: TableSchema = { name: "__empty", columns: [] };
  return {
    type: LogicalOperatorType.LOGICAL_GET,
    children: [],
    expressions: [],
    types: [],
    estimatedCardinality: 1,
    tableIndex,
    tableName: "__empty",
    schema: emptySchema,
    columnIds: [],
    tableFilters: [],
    getColumnBindings: () => [],
  };
}

export function makeGet(
  entry: BindingEntry,
  schema: TableSchema,
): BT.LogicalGet {
  const columnIds = schema.columns.map((_, i) => i);
  const types = schema.columns.map((c) => c.type);
  return {
    type: LogicalOperatorType.LOGICAL_GET,
    children: [],
    expressions: [],
    types,
    estimatedCardinality: 0,
    tableIndex: entry.tableIndex,
    tableName: schema.name,
    schema,
    columnIds,
    tableFilters: [],
    getColumnBindings: () =>
      columnIds.map((ci) => ({
        tableIndex: entry.tableIndex,
        columnIndex: ci,
      })),
  };
}

export function makeFilter(
  child: BT.LogicalOperator,
  exprs: BT.BoundExpression[],
): BT.LogicalFilter {
  return {
    type: LogicalOperatorType.LOGICAL_FILTER,
    children: [child],
    expressions: exprs,
    types: child.types,
    estimatedCardinality: 0,
    getColumnBindings: () => child.getColumnBindings(),
  };
}

export function makeDistinct(child: BT.LogicalOperator): BT.LogicalDistinct {
  return {
    type: LogicalOperatorType.LOGICAL_DISTINCT,
    children: [child],
    expressions: [],
    types: child.types,
    estimatedCardinality: 0,
    getColumnBindings: () => child.getColumnBindings(),
  };
}

export function makeOrderBy(
  child: BT.LogicalOperator,
  boundOrders: BT.BoundOrderByNode[],
): BT.LogicalOrderBy {
  return {
    type: LogicalOperatorType.LOGICAL_ORDER_BY,
    children: [child],
    orders: boundOrders,
    expressions: [],
    types: child.types,
    estimatedCardinality: 0,
    getColumnBindings: () => child.getColumnBindings(),
  };
}

export function makeLimit(
  child: BT.LogicalOperator,
  limitVal: number | null,
  offsetVal: number,
): BT.LogicalLimit {
  return {
    type: LogicalOperatorType.LOGICAL_LIMIT,
    children: [child],
    limitVal,
    offsetVal,
    expressions: [],
    types: child.types,
    estimatedCardinality: 0,
    getColumnBindings: () => child.getColumnBindings(),
  };
}
