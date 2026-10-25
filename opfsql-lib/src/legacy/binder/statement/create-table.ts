import { SemanticError } from "../../../errors.js";
import type { CreateTableStatement } from "../../../sql/parser/types.js";
import type { ColumnDef, TableSchema } from "../../../store/types.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import { mapTypeName } from "../core/type-map.js";
import { columnConstraints } from "../core/utils/column-constraints.js";
import { evalDefaultValue } from "../core/utils/eval-constant.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";

export function bindCreateTable(
  _ctx: BindContext,
  stmt: CreateTableStatement,
): BT.LogicalCreateTable {
  checkOnePrimaryKey(stmt);
  const columns: ColumnDef[] = stmt.columns.map((col) => {
    const constraints = columnConstraints(col);
    const isPk =
      constraints.primaryKeyCount > 0 ||
      stmt.primaryKeys.some((key) =>
        key.some((pk) => pk.toLowerCase() === col.name.toLowerCase()),
      );
    const colType = mapTypeName(col.type);

    if (colType === "JSON" || colType === "BLOB") {
      if (isPk) {
        throw new BindError(
          `${colType} column "${col.name}" cannot be a PRIMARY KEY`,
        );
      }
      if (constraints.isUnique) {
        throw new BindError(`${colType} column "${col.name}" cannot be UNIQUE`);
      }
    }

    if (constraints.isAutoincrement) {
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
      nullable: !constraints.isNotNull && !isPk,
      primaryKey: isPk,
      unique: constraints.isUnique,
      autoIncrement: constraints.isAutoincrement,
      defaultValue: constraints.defaultValue
        ? evalDefaultValue(col.name, constraints.defaultValue)
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

/** A table has at most one primary key, however it is written. */
function checkOnePrimaryKey(statement: CreateTableStatement): void {
  const columnKeys = statement.columns.reduce(
    (count, column) => count + columnConstraints(column).primaryKeyCount,
    0,
  );
  if (columnKeys + statement.primaryKeys.length > 1) {
    throw new SemanticError(
      `table "${statement.table}" has more than one primary key`,
    );
  }
}
