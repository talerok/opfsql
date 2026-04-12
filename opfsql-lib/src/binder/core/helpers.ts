import type {
  ConstantExpression,
  ParsedExpression,
} from "../../parser/types.js";
import { ExpressionClass } from "../../parser/types.js";
import type { TableSchema } from "../../store/types.js";
import type { BindContext } from "./context.js";
import { BindError } from "./errors.js";

export function requireTable(ctx: BindContext, name: string): TableSchema {
  const schema = ctx.catalog.getTable(name);
  if (!schema) {
    throw new BindError(`Table "${name}" not found`);
  }
  return schema;
}

export function evalConstantInt(expr: ParsedExpression): number {
  if (expr.expression_class === ExpressionClass.CONSTANT) {
    const c = expr as ConstantExpression;
    if (typeof c.value.value === "number") {
      if (c.value.value < 0) {
        throw new BindError("Expected non-negative integer constant");
      }
      return c.value.value;
    }
  }
  throw new BindError("Expected integer constant");
}

export function evalConstantValue(
  expr: ParsedExpression,
): string | number | boolean | null {
  if (expr.expression_class === ExpressionClass.CONSTANT) {
    const c = expr as ConstantExpression;
    return c.value.value;
  }
  return null;
}
