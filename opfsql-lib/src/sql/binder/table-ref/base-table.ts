import type { BaseTableReference } from "../../parser/types.js";
import type { TableSchema } from "../../../store/types.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import type { BindScope } from "../core/scope.js";
import { extractColumnsFromPlan } from "../core/utils/extract-columns.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";

export function bindBaseTableRef(
  ctx: BindContext,
  ref: BaseTableReference,
  scope: BindScope,
): BT.LogicalOperator {
  const alias = ref.alias ?? ref.table;

  const cte = scope.getCTE(ref.table);
  if (cte) {
    const columns = extractColumnsFromPlan(cte.plan, cte.plan.types);
    if (cte.aliases.length > 0) {
      for (let i = 0; i < cte.aliases.length; i++) {
        columns[i] = { ...columns[i], name: cte.aliases[i] };
      }
    }
    const virtualSchema: TableSchema = { name: ref.table, columns };
    const entry = scope.addTable(ref.table, alias, virtualSchema);

    return {
      type: LogicalOperatorType.LOGICAL_CTE_REF,
      cteName: ref.table,
      cteIndex: cte.index,
      children: [],
      expressions: [],
      types: cte.plan.types,
      estimatedCardinality: 0,
      columnBindings: columns.map((_, i) => ({
        tableIndex: entry.tableIndex,
        columnIndex: i,
      })),
    } satisfies BT.LogicalCTERef;
  }

  const schema = ctx.catalog.getTable(ref.table);
  if (!schema) {
    throw new BindError(`Table "${ref.table}" not found`);
  }

  const entry = scope.addTable(ref.table, alias, schema);
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
    columnBindings: columnIds.map((ci) => ({
      tableIndex: entry.tableIndex,
      columnIndex: ci,
    })),
  } satisfies BT.LogicalGet;
}
