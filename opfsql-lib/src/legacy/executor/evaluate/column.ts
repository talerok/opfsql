import { InternalError } from "../../../errors.js";
import type {
  BoundJsonAccessExpression,
  ColumnBinding,
  JsonPathSegment,
} from "../../binder/types.js";
import {
  followPath,
  type Path,
  type PathStep,
} from "../../../functions/index.js";
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
  const path = jsonPath(expression.path);
  return (row) => {
    const value = column(row);
    // A BLOB is no JSON value; JSON null and a missing value are NULL.
    if (value instanceof Uint8Array) return null;
    return followPath(value, path) ?? null;
  };
}

/** The bound path as the JSON functions' path, converted once. */
function jsonPath(segments: readonly JsonPathSegment[]): Path {
  return segments.map((segment) => pathStep(segment));
}

function pathStep(segment: JsonPathSegment): PathStep {
  if (segment.type === "field") return { type: "key", key: segment.name };
  return { type: "index", index: segment.value, fromEnd: false };
}
