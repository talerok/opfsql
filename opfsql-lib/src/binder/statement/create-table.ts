import type { CreateTableStatement } from "../../parser/types.js";
import type { ColumnDef, TableSchema } from "../../store/types.js";
import type { BindContext } from "../core/context.js";
import { evalConstantValue } from "../core/helpers.js";
import { mapParserType } from "../core/type-map.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";

export function bindCreateTable(
  _ctx: BindContext,
  stmt: CreateTableStatement,
): BT.LogicalCreateTable {
  const columns: ColumnDef[] = stmt.columns.map((col) => ({
    name: col.name,
    type: mapParserType(col.type),
    nullable: !col.is_not_null && !col.is_primary_key,
    primaryKey:
      col.is_primary_key ||
      stmt.primary_key.some(
        (pk) => pk.toLowerCase() === col.name.toLowerCase(),
      ),
    unique: col.is_unique,
    defaultValue: col.default_value
      ? evalConstantValue(col.default_value)
      : null,
  }));

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
