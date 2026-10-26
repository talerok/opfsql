import type { Index, IndexKey, Schema, Table } from "../../schema/index.js";
import {
  type ColumnReference,
  type Expression,
  type logical,
  ROWID_COLUMN,
  type StaticType,
} from "../plan/index.js";

// ---------------------------------------------------------------------------
// Index key parts as the optimizer matches conditions and ORDER BY keys
// against them: the expression a part holds, over the columns of one `get`,
// and its type. The rowid is a key of its own: the table's B-tree is
// ordered by it.
// ---------------------------------------------------------------------------

/**
 * Binds an index's expression key — SQL text in the schema — as an
 * expression over the columns of `table` read as relation `tableIndex`, so
 * a condition or ORDER BY key written the same way matches it. The optimizer
 * cannot parse (docs/architecture.md, Modules): the engine passes the
 * binder's. Without one, expression keys match nothing.
 */
export type KeyBinder = (
  sql: string,
  table: Table,
  tableIndex: number,
) => logical.Expression;

export interface KeyPart {
  /** What the part holds, as an expression over the get's columns. */
  readonly expression: logical.Expression;
  /** The type of what the part holds: the column's, or the key expression's. */
  readonly type: StaticType;
  /**
   * True if no entry holds NULL here, so NULLS FIRST or LAST does not matter
   * when the part delivers an ORDER BY.
   */
  readonly isNeverNull: boolean;
}

export interface IndexKeys {
  readonly index: Index;
  readonly parts: readonly KeyPart[];
}

/**
 * The indexes of each table read with their key parts, bound once per
 * relation for one optimize call: a get's indexes are asked for several
 * times (its access path, its order, its MIN/MAX), and binding an
 * expression key parses its SQL.
 */
export class IndexCatalog {
  private readonly parts = new Map<
    Index,
    Map<number, readonly KeyPart[] | null>
  >();

  constructor(
    private readonly schema: Schema,
    private readonly bindKey: KeyBinder | null,
  ) {}

  /** The indexes of the get's table; without a binder, none with an expression key. */
  indexesOf(get: logical.Get): IndexKeys[] {
    const indexes: IndexKeys[] = [];
    for (const index of this.schema.indexesOf(get.table.name)) {
      const parts = this.partsOf(index, get);
      if (parts !== null) indexes.push({ index, parts });
    }
    return indexes;
  }

  private partsOf(index: Index, get: logical.Get): readonly KeyPart[] | null {
    const byRelation =
      this.parts.get(index) ?? new Map<number, readonly KeyPart[] | null>();
    this.parts.set(index, byRelation);
    const known = byRelation.get(get.tableIndex);
    if (known !== undefined) return known;
    const parts = this.bindParts(index, get);
    byRelation.set(get.tableIndex, parts);
    return parts;
  }

  private bindParts(index: Index, get: logical.Get): readonly KeyPart[] | null {
    const parts: KeyPart[] = [];
    for (const key of index.keys) {
      const part = this.keyPart(key, get);
      if (part === null) return null;
      parts.push(part);
    }
    return parts;
  }

  private keyPart(key: IndexKey, get: logical.Get): KeyPart | null {
    if (key.kind === "column") return columnPart(key.column, get);
    if (this.bindKey === null) return null;
    const expression = this.bindKey(key.sql, get.table, get.tableIndex);
    return { expression, type: key.type, isNeverNull: false };
  }
}

function columnPart(position: number, get: logical.Get): KeyPart | null {
  const column = get.table.columns[position];
  if (column === undefined) return null;
  const isRowid = position === get.table.rowidColumn;
  const expression: ColumnReference = {
    kind: "column",
    binding: { tableIndex: get.tableIndex, columnIndex: position },
    depth: 0,
    name: column.name,
    type: column.type,
  };
  return {
    expression,
    type: column.type,
    isNeverNull: column.isNotNull || isRowid,
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
