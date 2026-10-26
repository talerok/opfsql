// Decoding the rows of a 10 000-row products table (id, name, price,
// category), as a full scan and a GROUP BY read them: whole records, a few
// fields of each, and tuples straight from a table's leaves. Run it with
// `npx vitest bench --run --project default bench/record-decode` before
// and after a change to records, tables or scans: it should not get slower.
import { bench, describe } from "vitest";
import { MemoryPages } from "../src/storage/btree/tests/memory-pages.js";
import {
  decodeInto,
  decodeRecord,
  encodeRecord,
  prepareField,
  type RecordField,
} from "../src/storage/format/record.js";
import type { Value } from "../src/values/index.js";
import {
  ROWID_FIELD,
  TableWriter,
  tupleLayout,
} from "../src/storage/rows/table-store.js";

const ROWS = 10_000;
const CATEGORIES = ["Electronics", "Books", "Clothing", "Food", "Toys"];

/** The bench scenarios' product rows; the id is the rowid, not stored. */
function product(id: number): Value[] {
  const price = ((id * 7919) % 10_000) / 100;
  return [null, `Product ${String(id)}`, price, CATEGORIES[id % 5] ?? null];
}

const records = Array.from({ length: ROWS }, (_, id) =>
  encodeRecord(product(id).map((field) => prepareField(field))),
);

const pages = new MemoryPages(4096);
const table = new TableWriter(pages, TableWriter.create(pages), "products");
for (let id = 0; id < ROWS; id++) table.insert(id, product(id));
pages.commit();

/** price and category, into a tuple of two. */
const priceAndCategory = [undefined, undefined, 0, 1];
const everyColumn = tupleLayout(
  [ROWID_FIELD, 1, 2, 3],
  [null, null, null, null],
);
const twoColumns = tupleLayout([3, 2], [null, null]);

describe("records, 10k rows", () => {
  bench("every field", () => {
    for (const record of records) decodeRecord(record);
  });

  bench("price and category", () => {
    const target: RecordField[] = [];
    for (const record of records) decodeInto(record, priceAndCategory, target);
  });
});

/** Reads every tuple of the table under `layout`. */
function readAll(layout: typeof everyColumn): number {
  let count = 0;
  const tuples = table.tuples(layout, [-Infinity, Infinity], "ascending");
  for (const tuple of tuples) count += tuple.length;
  return count;
}

describe("a table's tuples, 10k rows", () => {
  bench("every column", () => {
    readAll(everyColumn);
  });

  bench("category and price", () => {
    readAll(twoColumns);
  });
});
