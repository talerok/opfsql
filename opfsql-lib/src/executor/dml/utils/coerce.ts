import type { TableSchema } from "../../../store/types.js";
import { castJson } from "../../evaluate/utils/cast.js";
import type { Value } from "../../types.js";

export function coerceJsonIfNeeded(
  value: Value,
  schema: TableSchema,
  colIndex: number,
): Value {
  const col = schema.columns[colIndex];
  if (col.type === "JSON" && typeof value === "string") {
    return castJson(value);
  }
  return value;
}
