import type { CreateTableStatement } from "../../parser/types.js";
import type { ColumnDef, TableSchema } from "../../../store/types.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import { mapTypeName } from "../core/type-map.js";
import { evalDefaultValue } from "../core/utils/eval-constant.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";

export function bindCreateTable(
  _ctx: BindContext,
  stmt: CreateTableStatement,
): BT.LogicalCreateTable {
  const columns: ColumnDef[] = stmt.columns.map((col) => {
    const isPk =
      col.isPrimaryKey ||
      stmt.primaryKey.some((pk) => pk.toLowerCase() === col.name.toLowerCase());
    const colType = mapTypeName(col.type);

    if (colType === "JSON" || colType === "BLOB") {
      if (isPk) {
        throw new BindError(
          `${colType} column "${col.name}" cannot be a PRIMARY KEY`,
        );
      }
      if (col.isUnique) {
        throw new BindError(`${colType} column "${col.name}" cannot be UNIQUE`);
      }
    }

    if (col.isAutoincrement) {
      if (!isPk) {
        throw new BindError(
          `AUTOINCREMENT is only allowed on PRIMARY KEY columns`,
        );
      }
      if (colType !== "INTEGER") {
        throw new BindError(`AUTOINCREMENT is only allowed on INTEGER columns`);
      }
    }

    return {
      name: col.name,
      type: colType,
      nullable: !col.isNotNull && !isPk,
      primaryKey: isPk,
      unique: col.isUnique,
      autoIncrement: col.isAutoincrement,
      defaultValue: col.defaultValue
        ? evalDefaultValue(col.name, col.defaultValue)
        : null,
    };
  });

  if (columns.filter((c) => c.autoIncrement).length > 1) {
    throw new BindError(`Table can have at most one AUTOINCREMENT column`);
  }

  const schema: TableSchema = { name: stmt.table, columns };

  return {
    type: LogicalOperatorType.LOGICAL_CREATE_TABLE,
    schema,
    ifNotExists: stmt.ifNotExists,
    children: [],
    expressions: [],
    types: [],
    estimatedCardinality: 0,
    columnBindings: [],
  };
}
