import { InternalError } from "../errors.js";
import { followPath, type PathStep } from "../functions/index.js";
import type {
  ColumnBinding,
  ColumnReference,
  JsonAccess,
  JsonPathStep,
} from "../sql/plan/index.js";
import type { CompiledExpression, Layout, Scope } from "./scope.js";

/**
 * A column, its position found once: in the row, or for depth d in the
 * row d query levels out, which is the same for every row here.
 */
export function compileColumn(
  reference: ColumnReference,
  scope: Scope,
): CompiledExpression {
  const { depth } = reference;
  if (depth === 0) {
    const position = positionIn(scope.layout, reference);
    return (row) => row[position] ?? null;
  }
  const position = positionIn(outerLayout(scope, reference), reference);
  const { outer } = scope.run;
  return () => outer.at(depth)[position] ?? null;
}

/** `column.key[0]…`: the JSON value there, or NULL if there is none. */
export function compileJsonAccess(
  expression: JsonAccess,
  scope: Scope,
): CompiledExpression {
  const column = compileColumn(expression.column, scope);
  const path = expression.path.map((step) => pathStep(step));
  return (row) => {
    const value = column(row);
    // A BLOB is no JSON value; JSON null and a missing value are NULL.
    if (value instanceof Uint8Array) return null;
    return followPath(value, path) ?? null;
  };
}

function positionIn(layout: Layout, reference: ColumnReference): number {
  const position = layout.findIndex((binding) =>
    isSameColumn(binding, reference.binding),
  );
  if (position === -1) {
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

function isSameColumn(binding: ColumnBinding, other: ColumnBinding): boolean {
  return (
    binding.tableIndex === other.tableIndex &&
    binding.columnIndex === other.columnIndex
  );
}

function describe({ name, binding }: ColumnReference): string {
  return `${name} (${String(binding.tableIndex)}.${String(binding.columnIndex)})`;
}

/** A step of the bound path as the JSON functions take it. */
function pathStep(step: JsonPathStep): PathStep {
  if (step.kind === "key") return { type: "key", key: step.key };
  return { type: "index", index: step.index, fromEnd: false };
}
