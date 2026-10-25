import type { TableSchema } from "../../../../store/types.js";
import {
  assignValue,
  sqlTypeOf,
  type Value,
} from "../../../../values/index.js";

/**
 * `value` converted for storage in column `columnIndex`, as Assignment in
 * docs/semantics.md defines it: lossless conversions only, else
 * DATATYPE_MISMATCH.
 */
export function assignToColumn(
  value: Value,
  schema: TableSchema,
  columnIndex: number,
): Value {
  const column = schema.columns[columnIndex];
  return assignValue(value, sqlTypeOf(column.type));
}
