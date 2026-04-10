import type { DeleteStatement } from '../../parser/types.js';
import type * as BT from '../types.js';
import { LogicalOperatorType } from '../types.js';
import type { BindContext } from '../core/context.js';
import { requireTable } from '../core/helpers.js';
import { makeGet, makeFilter } from '../core/operators.js';
import { bindExpression } from '../expression/index.js';

export function bindDelete(
  ctx: BindContext,
  stmt: DeleteStatement,
): BT.LogicalDelete {
  const schema = requireTable(ctx, stmt.table);
  const scope = ctx.createScope();
  const entry = scope.addTable(stmt.table, stmt.table, schema);

  let scan: BT.LogicalOperator = makeGet(entry, schema);

  if (stmt.where_clause) {
    const filter = bindExpression(ctx, stmt.where_clause, scope);
    scan = makeFilter(scan, [filter]);
  }

  return {
    type: LogicalOperatorType.LOGICAL_DELETE,
    tableName: schema.name,
    schema,
    children: [scan],
    expressions: [],
    types: [],
    estimatedCardinality: 0,
    getColumnBindings: () => [],
  };
}
