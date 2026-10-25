import { decodeCbor, encodeCbor } from "./page-codec.js";
import type { ColumnDef, Row, RowId, TableSchema, Value } from "./types.js";

// ---------------------------------------------------------------------------
// Row cells — how a table row is stored in a B-tree leaf.
//
// A row is a CBOR array of its column values in schema order, with no column
// names. ADD COLUMN appends to the schema, so rows written before it are
// shorter and read the new column as its default; DROP COLUMN rewrites the
// table without the dropped position. A column that aliases the rowid is
// stored as NULL: its value is the row's key.
// ---------------------------------------------------------------------------

/**
 * The column whose value is the rowid: an INTEGER PRIMARY KEY that is the
 * table's whole primary key. Rows are keyed by it instead of a hidden rowid.
 */
export function rowidColumn(schema: TableSchema): ColumnDef | null {
  let primaryKey: ColumnDef | null = null;
  for (const column of schema.columns) {
    if (!column.primaryKey) continue;
    if (primaryKey) return null;
    primaryKey = column;
  }
  return primaryKey?.type === "INTEGER" ? primaryKey : null;
}

export function encodeRow(schema: TableSchema, row: Row): Uint8Array {
  const rowid = rowidColumn(schema);
  return encodeCbor(
    schema.columns.map((column) =>
      column === rowid ? null : (row[column.name] ?? null),
    ),
  );
}

export function decodeRow(
  schema: TableSchema,
  rowId: RowId,
  cell: Uint8Array,
): Row {
  const rowid = rowidColumn(schema);
  const values = decodeCbor<Value[]>(cell);
  const row: Row = {};
  schema.columns.forEach((column, i) => {
    if (column === rowid) row[column.name] = rowId;
    else if (i < values.length) row[column.name] = detach(values[i]);
    else row[column.name] = column.defaultValue;
  });
  return row;
}

/** A decoded BLOB is a view of the stored page; a row gets its own copy. */
function detach(value: Value): Value {
  return value instanceof Uint8Array ? new Uint8Array(value) : value;
}
