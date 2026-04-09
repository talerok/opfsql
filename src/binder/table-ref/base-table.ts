import type { TableSchema } from '../../store/types.js';
import type { BaseTableRef } from '../../parser/types.js';
import type * as BT from '../types.js';
import { LogicalOperatorType } from '../types.js';
import type { BindContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { BindError } from '../core/errors.js';
import { extractColumnsFromPlan } from '../core/plan-utils.js';

export function bindBaseTableRef(
  ctx: BindContext,
  ref: BaseTableRef,
  scope: BindScope,
): BT.LogicalOperator {
  const alias = ref.alias ?? ref.table_name;

  const cte = scope.getCTE(ref.table_name);
  if (cte) {
    const columns = extractColumnsFromPlan(cte.plan, cte.plan.types);
    if (cte.aliases.length > 0) {
      for (let i = 0; i < cte.aliases.length; i++) {
        columns[i] = { ...columns[i], name: cte.aliases[i] };
      }
    }
    const virtualSchema: TableSchema = { name: ref.table_name, columns };
    const entry = scope.addTable(ref.table_name, alias, virtualSchema);

    return {
      type: LogicalOperatorType.LOGICAL_CTE_REF,
      cteName: ref.table_name,
      cteIndex: cte.index,
      children: [],
      expressions: [],
      types: cte.plan.types,
      estimatedCardinality: 0,
      getColumnBindings: () =>
        columns.map((_, i) => ({
          tableIndex: entry.tableIndex,
          columnIndex: i,
        })),
    } satisfies BT.LogicalCTERef;
  }

  const schema = ctx.catalog.getTable(ref.table_name);
  if (!schema) {
    throw new BindError(`Table "${ref.table_name}" not found`);
  }

  const entry = scope.addTable(ref.table_name, alias, schema);
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
  } satisfies BT.LogicalGet;
}
