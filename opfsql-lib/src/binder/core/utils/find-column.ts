import type { TableSchema } from "../../../store/types.js";
import { BindError } from "../errors.js";

export function findColumnIndex(schema: TableSchema, columnName: string): number {
  const lower = columnName.toLowerCase();
  return schema.columns.findIndex((c) => c.name.toLowerCase() === lower);
}

export function findColumnIndexOrThrow(
  schema: TableSchema,
  columnName: string,
): number {
  const idx = findColumnIndex(schema, columnName);
  if (idx === -1) {
    throw new BindError(`Column "${columnName}" not found in table "${schema.name}"`);
  }
  return idx;
}

export function getPrimaryKeyColumns(schema: TableSchema): number[] {
  return schema.columns
    .map((c, i) => (c.primaryKey ? i : -1))
    .filter((i) => i !== -1);
}
