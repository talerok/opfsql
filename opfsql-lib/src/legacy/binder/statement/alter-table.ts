import {
  AlterType,
  type AlterTableStatement,
  type ColumnDefinition,
} from "../../../sql/parser/types.js";
import { getIndexColumns } from "../../../store/index-expression.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import { mapTypeName } from "../core/type-map.js";
import { columnConstraints } from "../core/utils/column-constraints.js";
import { evalDefaultValue } from "../core/utils/eval-constant.js";
import { requireTable } from "../core/utils/require-table.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";

export function bindAlterTable(
  ctx: BindContext,
  stmt: AlterTableStatement,
): BT.LogicalAlterTable {
  const table = requireTable(ctx, stmt.table);
  const action =
    stmt.alterType === AlterType.ADD_COLUMN
      ? bindAddColumn(stmt.column)
      : bindDropColumn(ctx, stmt.table, stmt.columnName);

  return {
    type: LogicalOperatorType.LOGICAL_ALTER_TABLE,
    tableName: stmt.table,
    action,
    children: [],
    expressions: [],
    types: [],
    estimatedCardinality: 0,
    columnBindings: [],
  };
}

function bindAddColumn(def: ColumnDefinition): BT.LogicalAlterTable["action"] {
  const constraints = columnConstraints(def);
  // A table's primary key, and with it how its rows are keyed, is fixed
  // when the table is created.
  if (constraints.primaryKeyCount > 0) {
    throw new BindError(`Cannot add a PRIMARY KEY column`);
  }
  if (constraints.isAutoincrement) {
    throw new BindError(`AUTOINCREMENT is only allowed on PRIMARY KEY columns`);
  }

  return {
    type: "ADD_COLUMN",
    column: {
      name: def.name,
      type: mapTypeName(def.type),
      nullable: !constraints.isNotNull,
      primaryKey: false,
      unique: constraints.isUnique,
      autoIncrement: false,
      defaultValue: constraints.defaultValue
        ? evalDefaultValue(def.name, constraints.defaultValue)
        : null,
    },
  };
}

function bindDropColumn(
  ctx: BindContext,
  tableName: string,
  colName: string,
): BT.LogicalAlterTable["action"] {
  for (const idx of ctx.catalog.getTableIndexes(tableName)) {
    const referencedCols = idx.expressions.flatMap(getIndexColumns);
    if (referencedCols.some((c) => c.toLowerCase() === colName.toLowerCase())) {
      throw new BindError(
        `Cannot drop column "${colName}": referenced by index "${idx.name}"`,
      );
    }
  }
  return { type: "DROP_COLUMN", columnName: colName };
}
