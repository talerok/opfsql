import type { AlterTableStatement } from "../../parser/types.js";
import { getIndexColumns } from "../../store/index-expression.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import { mapParserType } from "../core/type-map.js";
import { evalConstantValue } from "../core/utils/eval-constant.js";
import { requireTable } from "../core/utils/require-table.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";

export function bindAlterTable(
  ctx: BindContext,
  stmt: AlterTableStatement,
): BT.LogicalAlterTable {
  const table = requireTable(ctx, stmt.table);
  const action =
    stmt.alter_type === "ADD_COLUMN" && stmt.column_def
      ? bindAddColumn(stmt.column_def, table)
      : bindDropColumn(ctx, stmt.table, stmt.column_name!);

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
  def: NonNullable<AlterTableStatement["column_def"]>,
  table: BT.TableSchema,
): BT.LogicalAlterTable["action"] {
  const colType = mapParserType(def.type);

  if (def.is_autoincrement) {
    if (!def.is_primary_key) {
      throw new BindError(
        `AUTOINCREMENT is only allowed on PRIMARY KEY columns`,
      );
    }
    if (colType !== "INTEGER") {
      throw new BindError(`AUTOINCREMENT is only allowed on INTEGER columns`);
    }
    if (table.columns.some((c) => c.autoIncrement)) {
      throw new BindError(`Table already has an AUTOINCREMENT column`);
    }
  }

  return {
    type: "ADD_COLUMN",
    column: {
      name: def.name,
      type: colType,
      nullable: !def.is_not_null,
      primaryKey: def.is_primary_key,
      unique: def.is_unique,
      autoIncrement: def.is_autoincrement,
      defaultValue: def.default_value
        ? evalConstantValue(def.default_value)
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
