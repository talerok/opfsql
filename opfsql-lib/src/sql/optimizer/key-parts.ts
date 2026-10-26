import type { Index, IndexKey } from "../../schema/index.js";
import type { ColumnReference, Expression, logical } from "../plan/index.js";
import { ROWID_COLUMN } from "../plan/index.js";
import type { Planner } from "./planner.js";

// ---------------------------------------------------------------------------
// Index key parts as the optimizer matches conditions and ORDER BY keys
// against them: the expression a part holds, over the columns of one `get`,
// and its type. The rowid is a key of its own: the table's B-tree is
// ordered by it.
// ---------------------------------------------------------------------------

type StaticType = ColumnReference["type"];

export interface KeyPart {
  /** What the part holds, as an expression over the get's columns. */
  readonly expression: logical.Expression;
  /** The type of what the part holds: the column's, or the key expression's. */
  readonly type: StaticType;
  /** True if no entry holds NULL here, so a range below a value skips none. */
  readonly isNeverNull: boolean;
}

export interface IndexKeys {
  readonly index: Index;
  readonly parts: readonly KeyPart[];
}

/** The indexes of the get's table, each with its parts; an expression key unbound without a binder. */
export function indexesOf(get: logical.Get, planner: Planner): IndexKeys[] {
  const indexes: IndexKeys[] = [];
  for (const index of planner.schema.indexesOf(get.table.name)) {
    const parts = index.keys.map((key) => keyPart(key, get, planner));
    if (parts.every((part) => part !== null)) indexes.push({ index, parts });
  }
  return indexes;
}

function keyPart(
  key: IndexKey,
  get: logical.Get,
  planner: Planner,
): KeyPart | null {
  if (key.kind === "expression") {
    if (planner.bindKey === null) return null;
    const expression = planner.bindKey(key.sql, get.table, get.tableIndex);
    return { expression, type: key.type, isNeverNull: false };
  }
  const column = get.table.columns[key.column];
  if (column === undefined) return null;
  const isRowid = key.column === get.table.rowidColumn;
  return {
    expression: columnOf(get, key.column),
    type: column.type,
    isNeverNull: column.isNotNull || isRowid,
  };
}

/** The rowid as a key part: never NULL, and INTEGER. */
export function rowidPart(get: logical.Get): KeyPart {
  return {
    expression: columnOf(get, ROWID_COLUMN),
    type: "INTEGER",
    isNeverNull: true,
  };
}

/** True if `expression` reads the get's rowid: ROWID_COLUMN, or its INTEGER PRIMARY KEY. */
export function isRowidOf<Plan>(
  expression: Expression<Plan>,
  get: logical.Get,
): boolean {
  if (expression.kind !== "column" || expression.depth !== 0) return false;
  const { tableIndex, columnIndex } = expression.binding;
  const isRowidColumn =
    columnIndex === ROWID_COLUMN || columnIndex === get.table.rowidColumn;
  return tableIndex === get.tableIndex && isRowidColumn;
}

function columnOf(get: logical.Get, columnIndex: number): ColumnReference {
  const isRowid = columnIndex === ROWID_COLUMN;
  const column = isRowid ? undefined : get.table.columns[columnIndex];
  return {
    kind: "column",
    binding: { tableIndex: get.tableIndex, columnIndex },
    depth: 0,
    name: column?.name ?? "rowid",
    type: column?.type ?? "INTEGER",
  };
}
