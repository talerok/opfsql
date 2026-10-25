import { InternalError } from "../../errors.js";
import type {
  BoundJsonAccessExpression,
  ColumnBinding,
  JsonPathSegment,
} from "../../sql/binder/types.js";
import type { JsonValue, Value } from "../../values/index.js";
import type { CompiledExpression, Scope } from "./scope.js";

/**
 * A column of the row, found once. A column the row lacks is one of the
 * outer row of a correlated subquery: the same for every row here.
 */
export function compileColumn(
  binding: ColumnBinding,
  scope: Scope,
): CompiledExpression {
  const position = scope.resolver(binding);
  if (position !== undefined) return (row) => row[position] ?? null;
  const { outer } = scope.context;
  const outerPosition = outer?.resolver(binding);
  if (outer === null || outerPosition === undefined) {
    throw new InternalError(
      `column ${String(binding.tableIndex)}.${String(binding.columnIndex)} is not in the row`,
    );
  }
  const value = outer.tuple[outerPosition] ?? null;
  return () => value;
}

/** `column.path[0].key`: the JSON value there, or NULL if there is none. */
export function compileJsonAccess(
  expression: BoundJsonAccessExpression,
  scope: Scope,
): CompiledExpression {
  const column = compileColumn(expression.child.binding, scope);
  const { path } = expression;
  return (row) => followJsonPath(column(row), path);
}

export function followJsonPath(
  value: Value,
  path: readonly JsonPathSegment[],
): Value {
  let current: JsonValue | undefined =
    value instanceof Uint8Array ? null : value;
  for (const segment of path) {
    if (current === null || current === undefined) return null;
    current = step(current, segment);
  }
  return current ?? null;
}

function step(
  value: JsonValue,
  segment: JsonPathSegment,
): JsonValue | undefined {
  if (segment.type === "index") {
    return Array.isArray(value) ? value[segment.value] : undefined;
  }
  return typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.hasOwn(value, segment.name)
    ? value[segment.name]
    : undefined;
}
