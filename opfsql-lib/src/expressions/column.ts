import { InternalError } from "../errors.js";
import { followPath } from "../functions/index.js";
import {
  type ColumnReference,
  type JsonAccess,
  positionIn,
} from "../sql/plan/index.js";
import type { CompiledExpression, Layout, Scope, Tuple } from "./scope.js";

/**
 * A column, its position found once: in the row, or for depth d in the
 * row d query levels out, which is the same for every row here. A row
 * without a value there is narrower than its layout: an InternalError, not
 * a NULL.
 */
export function compileColumn(
  reference: ColumnReference,
  scope: Scope,
): CompiledExpression {
  const { depth } = reference;
  if (depth === 0) {
    const position = positionOf(scope.layout, reference);
    return (row) => {
      const value = row[position];
      if (value === undefined) throw narrowRow(reference, row);
      return value;
    };
  }
  const position = positionOf(outerLayout(scope, reference), reference);
  const { outer } = scope.run;
  return () => {
    const row = outer.at(depth);
    const value = row[position];
    if (value === undefined) throw narrowRow(reference, row);
    return value;
  };
}

function narrowRow(reference: ColumnReference, row: Tuple): InternalError {
  return new InternalError(
    `column ${describe(reference)} is past the end of a row of ${String(row.length)} values`,
  );
}

/** `column.key[0]…`: the JSON value there, or NULL if there is none. */
export function compileJsonAccess(
  expression: JsonAccess,
  scope: Scope,
): CompiledExpression {
  const column = compileColumn(expression.column, scope);
  const { path } = expression;
  return (row) => {
    const value = column(row);
    // A BLOB is no JSON value; JSON null and a missing value are NULL.
    if (value instanceof Uint8Array) return null;
    return followPath(value, path) ?? null;
  };
}

function positionOf(layout: Layout, reference: ColumnReference): number {
  const position = positionIn(layout, reference.binding);
  if (position === null) {
    throw new InternalError(`column ${describe(reference)} is not in the row`);
  }
  return position;
}

function outerLayout(scope: Scope, reference: ColumnReference): Layout {
  const layout = scope.outerLayouts[reference.depth - 1];
  if (layout === undefined) {
    throw new InternalError(
      `column ${describe(reference)} is ${String(reference.depth)} levels out, in a query ${String(scope.outerLayouts.length)} deep`,
    );
  }
  return layout;
}

function describe({ name, binding }: ColumnReference): string {
  return `${name} (${String(binding.tableIndex)}.${String(binding.columnIndex)})`;
}
