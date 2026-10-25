import { decodeCbor, encodeCbor } from "./page-codec.js";
import type { Row, TableSchema, Value } from "./types.js";

// ---------------------------------------------------------------------------
// Row cells — how a table row is stored in a B-tree leaf.
//
// A row is a CBOR array of its column values in schema order, with no column
// names. ADD COLUMN appends to the schema, so rows written before it are
// shorter and read the new column as its default; DROP COLUMN rewrites the
// table without the dropped position.
// ---------------------------------------------------------------------------

export function encodeRow(schema: TableSchema, row: Row): Uint8Array {
  return encodeCbor(schema.columns.map((column) => row[column.name] ?? null));
}

export function decodeRow(schema: TableSchema, cell: Uint8Array): Row {
  const values = decodeCbor<Value[]>(cell);
  const row: Row = {};
  schema.columns.forEach((column, i) => {
    row[column.name] =
      i < values.length ? detach(values[i]) : column.defaultValue;
  });
  return row;
}

/** A decoded BLOB is a view of the stored page; a row gets its own copy. */
function detach(value: Value): Value {
  return value instanceof Uint8Array ? new Uint8Array(value) : value;
}
