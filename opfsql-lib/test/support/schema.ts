import type { Column } from "../../src/schema/index.js";
import type { SqlType } from "../../src/values/index.js";

/** A column for schema-model tests: nullable, no default, unless `extra` says. */
export function column(
  name: string,
  type: SqlType = "TEXT",
  extra: Partial<Column> = {},
): Column {
  return { name, type, isNotNull: false, defaultValue: null, ...extra };
}
