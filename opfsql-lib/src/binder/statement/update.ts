import type { UpdateStatement } from '../../parser/types.js';
import type * as BT from '../types.js';
import { LogicalOperatorType } from '../types.js';
import { BindError } from '../core/errors.js';
import type { BindContext } from '../core/context.js';
import { requireTable } from '../core/helpers.js';
import { makeGet, makeFilter } from '../core/operators.js';
import { bindExpression } from '../expression/index.js';

export function bindUpdate(
  ctx: BindContext,
  stmt: UpdateStatement,
): BT.LogicalUpdate {
  const schema = requireTable(ctx, stmt.table);
  const scope = ctx.createScope();
  const entry = scope.addTable(stmt.table, stmt.table, schema);

  let scan: BT.LogicalOperator = makeGet(entry, schema);

  if (stmt.where_clause) {
    const filter = bindExpression(ctx, stmt.where_clause, scope);
    scan = makeFilter(scan, [filter]);
  }

  const updateColumns: number[] = [];
  const expressions: BT.BoundExpression[] = [];
  for (const clause of stmt.set_clauses) {
    const idx = schema.columns.findIndex(
      (c) => c.name.toLowerCase() === clause.column.toLowerCase(),
    );
    if (idx === -1) {
      throw new BindError(
        `Column "${clause.column}" not found in table "${stmt.table}"`,
      );
    }
    updateColumns.push(idx);
    expressions.push(bindExpression(ctx, clause.value, scope));
  }

  return {
    type: LogicalOperatorType.LOGICAL_UPDATE,
    tableName: schema.name,
    schema,
    children: [scan],
    updateColumns,
    expressions,
    types: [],
    estimatedCardinality: 0,
    getColumnBindings: () => [],
  };
}
