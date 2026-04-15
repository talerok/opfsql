import type { AlterTableStatement } from '../../parser/types.js';
import type * as BT from '../types.js';
import { LogicalOperatorType } from '../types.js';
import type { BindContext } from '../core/context.js';
import { BindError } from '../core/errors.js';
import { requireTable } from '../core/utils/require-table.js';
import { mapParserType } from '../core/type-map.js';
import { evalConstantValue } from '../core/utils/eval-constant.js';

export function bindAlterTable(
  ctx: BindContext,
  stmt: AlterTableStatement,
): BT.LogicalAlterTable {
  const table = requireTable(ctx, stmt.table);

  let action: BT.LogicalAlterTable['action'];
  if (stmt.alter_type === 'ADD_COLUMN' && stmt.column_def) {
    const colType = mapParserType(stmt.column_def.type);

    if (stmt.column_def.is_autoincrement) {
      if (!stmt.column_def.is_primary_key) {
        throw new BindError(`AUTOINCREMENT is only allowed on PRIMARY KEY columns`);
      }
      if (colType !== 'INTEGER') {
        throw new BindError(`AUTOINCREMENT is only allowed on INTEGER columns`);
      }
      if (table.columns.some((c) => c.autoIncrement)) {
        throw new BindError(`Table already has an AUTOINCREMENT column`);
      }
    }

    action = {
      type: 'ADD_COLUMN',
      column: {
        name: stmt.column_def.name,
        type: colType,
        nullable: !stmt.column_def.is_not_null,
        primaryKey: stmt.column_def.is_primary_key,
        unique: stmt.column_def.is_unique,
        autoIncrement: stmt.column_def.is_autoincrement,
        defaultValue: stmt.column_def.default_value
          ? evalConstantValue(stmt.column_def.default_value)
          : null,
      },
    };
  } else {
    const colName = stmt.column_name!;
    for (const idx of ctx.catalog.getTableIndexes(stmt.table)) {
      if (idx.columns.some((c) => c.toLowerCase() === colName.toLowerCase())) {
        throw new BindError(
          `Cannot drop column "${colName}": referenced by index "${idx.name}"`,
        );
      }
    }
    action = {
      type: 'DROP_COLUMN',
      columnName: colName,
    };
  }

  return {
    type: LogicalOperatorType.LOGICAL_ALTER_TABLE,
    tableName: stmt.table,
    action,
    children: [],
    expressions: [],
    types: [],
    estimatedCardinality: 0,
    getColumnBindings: () => [],
  };
}
