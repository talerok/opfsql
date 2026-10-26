import { SemanticError } from "../../errors.js";
import { nameKey } from "../../schema/index.js";
import {
  logical,
  type ColumnReference,
  type OutputColumn,
} from "../plan/index.js";

// ---------------------------------------------------------------------------
// Columns: of a table or a statement's column list, found by nameKey and
// each named once; of a plan node, read by references to its layout.
// ---------------------------------------------------------------------------

/** The position of the column called `name` among `columns`; -1 if none. */
export function findColumn(
  columns: readonly { readonly name: string }[],
  name: string,
): number {
  const key = nameKey(name);
  return columns.findIndex((column) => nameKey(column.name) === key);
}

/** The position of table `table`'s column called `name`; it must have one. */
export function columnIn(
  columns: readonly { readonly name: string }[],
  name: string,
  table: string,
): number {
  const position = findColumn(columns, name);
  if (position === -1) {
    throw new SemanticError(`table ${table} has no column named ${name}`);
  }
  return position;
}

/** The position of the first value met before; -1 if each is met once. */
export function firstRepeated(values: readonly unknown[]): number {
  return values.findIndex(
    (value, position) => values.indexOf(value) !== position,
  );
}

/** The name a reference to `expression`'s column carries: a column's own, else `fallback`. */
export function nameOf(
  expression: logical.Expression,
  fallback: string,
): string {
  return expression.kind === "column" ? expression.name : fallback;
}

/** References to every column of `plan`, in its layout order. */
export function readColumns(
  plan: logical.Plan,
  columns: readonly OutputColumn[],
): ColumnReference[] {
  return logical.layoutOf(plan).map((binding, position) => ({
    kind: "column",
    binding,
    depth: 0,
    name: columns[position]?.name ?? String(position),
    type: columns[position]?.type ?? "ANY",
  }));
}
