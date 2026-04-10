import type { ColumnDef, TableSchema } from '../../store/types.js';
import type { SubqueryRef } from '../../parser/types.js';
import type * as BT from '../types.js';
import { LogicalOperatorType } from '../types.js';
import type { BindContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { extractColumnsFromPlan } from '../core/plan-utils.js';
import { bindQueryNode } from '../statement/query-node.js';

export function bindSubqueryRef(
  ctx: BindContext,
  ref: SubqueryRef,
  scope: BindScope,
): BT.LogicalOperator {
  const childScope = scope.createIsolatedScope();
  const subplan = bindQueryNode(ctx, ref.subquery.node, childScope);

  const alias = ref.alias ?? '__subquery';
  const inferredColumns = extractColumnsFromPlan(subplan, subplan.types);
  const columns: ColumnDef[] = subplan.types.map((t, i) => ({
    name: ref.column_name_alias[i] ?? inferredColumns[i]?.name ?? `column${i}`,
    type: t,
    nullable: true,
    primaryKey: false,
    unique: false,
    defaultValue: null,
  }));
  const virtualSchema: TableSchema = { name: alias, columns };
  const entry = scope.addTable(alias, alias, virtualSchema);

  const columnIds = columns.map((_, i) => i);
  return {
    type: LogicalOperatorType.LOGICAL_GET,
    children: [subplan],
    expressions: [],
    types: subplan.types,
    estimatedCardinality: 0,
    tableIndex: entry.tableIndex,
    tableName: alias,
    schema: virtualSchema,
    columnIds,
    tableFilters: [],
    getColumnBindings: () =>
      columnIds.map((ci) => ({
        tableIndex: entry.tableIndex,
        columnIndex: ci,
      })),
  } satisfies BT.LogicalGet;
}
