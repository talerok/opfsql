import type { AlterTableStatement } from "../../parser/types.js";
import { getIndexColumns } from "../../../store/index-expression.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import { mapTypeName } from "../core/type-map.js";
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
    stmt.alterType === "ADD_COLUMN" && stmt.columnDefinition
      ? bindAddColumn(stmt.columnDefinition)
      : bindDropColumn(ctx, stmt.table, stmt.columnName!);

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

function bindAddColumn(
  def: NonNullable<AlterTableStatement["columnDefinition"]>,
): BT.LogicalAlterTable["action"] {
  // A table's primary key, and with it how its rows are keyed, is fixed
  // when the table is created.
  if (def.isPrimaryKey) {
    throw new BindError(`Cannot add a PRIMARY KEY column`);
  }
  if (def.isAutoincrement) {
    throw new BindError(`AUTOINCREMENT is only allowed on PRIMARY KEY columns`);
  }

  return {
    type: "ADD_COLUMN",
    column: {
      name: def.name,
      type: mapTypeName(def.type),
      nullable: !def.isNotNull,
      primaryKey: false,
      unique: def.isUnique,
      autoIncrement: false,
      defaultValue: def.defaultValue
        ? evalDefaultValue(def.name, def.defaultValue)
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
