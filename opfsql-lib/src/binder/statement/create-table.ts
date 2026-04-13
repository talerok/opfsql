import type { CreateTableStatement } from "../../parser/types.js";
import type { ColumnDef, TableSchema } from "../../store/types.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import { evalConstantValue } from "../core/helpers.js";
import { mapParserType } from "../core/type-map.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";

export function bindCreateTable(
  _ctx: BindContext,
  stmt: CreateTableStatement,
): BT.LogicalCreateTable {
  const columns: ColumnDef[] = stmt.columns.map((col) => {
    const isPk =
      col.is_primary_key ||
      stmt.primary_key.some(
        (pk) => pk.toLowerCase() === col.name.toLowerCase(),
      );
    const colType = mapParserType(col.type);

    if (colType === "JSON") {
      if (isPk) {
        throw new BindError(`JSON column "${col.name}" cannot be a PRIMARY KEY`);
      }
      if (col.is_unique) {
        throw new BindError(`JSON column "${col.name}" cannot be UNIQUE`);
      }
    }

    if (col.is_autoincrement) {
      if (!isPk) {
        throw new BindError(
          `AUTOINCREMENT is only allowed on PRIMARY KEY columns`,
        );
      }
      if (colType !== "INTEGER") {
        throw new BindError(
          `AUTOINCREMENT is only allowed on INTEGER columns`,
        );
      }
    }

    return {
      name: col.name,
      type: colType,
      nullable: !col.is_not_null && !isPk,
      primaryKey: isPk,
      unique: col.is_unique,
      autoIncrement: col.is_autoincrement,
      defaultValue: col.default_value
        ? evalConstantValue(col.default_value)
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
    ifNotExists: stmt.if_not_exists,
    children: [],
    expressions: [],
    types: [],
    estimatedCardinality: 0,
    getColumnBindings: () => [],
  };
}
