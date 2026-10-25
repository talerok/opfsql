import type { TableSchema } from "../../../store/types.js";
import { assignValue, type Value } from "../../../values/index.js";

/** Text stored in a JSON column is parsed as JSON text. */
export function coerceJsonIfNeeded(
  value: Value,
  schema: TableSchema,
  columnIndex: number,
): Value {
  return schema.columns[columnIndex]?.type === "JSON"
    ? assignValue(value, "JSON")
    : value;
}
