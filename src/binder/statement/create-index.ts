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

  // Resolve column names to canonical form (ColumnDef.name) for consistent
  // key building across DDL backfill and DML index maintenance.
  const resolvedColumns: string[] = [];
  for (const col of stmt.columns) {
    const def = schema.columns.find(
      (c) => c.name.toLowerCase() === col.toLowerCase(),
    );
    if (!def) {
      throw new BindError(
        `Column "${col}" not found in table "${stmt.table_name}"`,
      );
    }
    resolvedColumns.push(def.name);
  }

  return {
    type: LogicalOperatorType.LOGICAL_CREATE_INDEX,
    index: {
      name: stmt.index_name,
      tableName: stmt.table_name,
      columns: resolvedColumns,
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
