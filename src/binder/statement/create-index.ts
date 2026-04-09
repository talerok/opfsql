import type { CreateIndexStatement } from '../../parser/types.js';
import type * as BT from '../types.js';
import { LogicalOperatorType } from '../types.js';
import { BindError } from '../core/errors.js';
import type { BindContext } from '../core/context.js';
import { requireTable } from '../core/helpers.js';

export function bindCreateIndex(
  ctx: BindContext,
  stmt: CreateIndexStatement,
): BT.LogicalCreateIndex {
  const schema = requireTable(ctx, stmt.table_name);

  for (const col of stmt.columns) {
    const found = schema.columns.some(
      (c) => c.name.toLowerCase() === col.toLowerCase(),
    );
    if (!found) {
      throw new BindError(
        `Column "${col}" not found in table "${stmt.table_name}"`,
      );
    }
  }

  return {
    type: LogicalOperatorType.LOGICAL_CREATE_INDEX,
    index: {
      name: stmt.index_name,
      tableName: stmt.table_name,
      columns: stmt.columns,
      unique: stmt.is_unique,
    },
    ifNotExists: stmt.if_not_exists,
    children: [],
    expressions: [],
    types: [],
    estimatedCardinality: 0,
    getColumnBindings: () => [],
  };
}
