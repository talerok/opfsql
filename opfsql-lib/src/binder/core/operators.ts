import type { LogicalType, TableSchema } from "../../store/types.js";
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

export function makeAggregate(
  child: BT.LogicalOperator,
  groups: BT.BoundExpression[],
  aggregates: BT.BoundAggregateExpression[],
  groupIndex: number,
  aggregateIndex: number,
  havingExpression: BT.BoundExpression | null,
): BT.LogicalAggregate {
  const bindings: BT.ColumnBinding[] = [
    ...groups.map((_, i) => ({ tableIndex: groupIndex, columnIndex: i })),
    ...aggregates.map((_, i) => ({ tableIndex: aggregateIndex, columnIndex: i })),
  ];
  return {
    type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
    groupIndex,
    aggregateIndex,
    children: [child],
    expressions: aggregates,
    groups,
    havingExpression,
    types: [
      ...groups.map((g) => g.returnType),
      ...aggregates.map((a) => a.returnType),
    ],
    estimatedCardinality: 0,
    getColumnBindings: () => bindings,
  };
}

export function makeProjection(
  child: BT.LogicalOperator,
  tableIndex: number,
  expressions: BT.BoundExpression[],
  aliases: (string | null)[],
): BT.LogicalProjection {
  const types: LogicalType[] = expressions.map((e) => e.returnType);
  const bindings: BT.ColumnBinding[] = expressions.map((_, i) => ({
    tableIndex,
    columnIndex: i,
  }));
  return {
    type: LogicalOperatorType.LOGICAL_PROJECTION,
    tableIndex,
    children: [child],
    expressions,
    aliases,
    types,
    estimatedCardinality: 0,
    getColumnBindings: () => bindings,
  };
}
