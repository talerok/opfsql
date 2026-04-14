import type { TableSchema } from "../../../store/types.js";
import type { BindContext } from "../context.js";
import { BindError } from "../errors.js";

export function requireTable(ctx: BindContext, name: string): TableSchema {
  const schema = ctx.catalog.getTable(name);
  if (!schema) {
    throw new BindError(`Table "${name}" not found`);
  }
  return schema;
}
