import { describe, expect, it } from "vitest";
import { mulberry32 } from "../../../../test/support/random.js";
import { randomValue } from "../../../../test/support/values.js";
import { IndexKeyTooLargeError, RowTooLargeError } from "../../../errors.js";
import type { Value } from "../../../values/index.js";
import { compareBytes, rowKey } from "../../../values/index.js";
import type { PageReader } from "../../btree/page-store.js";
import { MemoryPages } from "../../btree/tests/memory-pages.js";
import { encodeKey, keyRowid, prefixEnd } from "../../format/key.js";
import { IndexWriter } from "../index-store.js";
import {
  ROWID_FIELD,
  type RowOrder,
  type StoredRow,
  TableReader,
  TableWriter,
  tupleLayout,
} from "../table-store.js";

const PAGE_SIZE = 4096;

function setup() {
  const pages = new MemoryPages(PAGE_SIZE);
  const root = TableWriter.create(pages);
  pages.commit();
  return { pages, root, table: new TableWriter(pages, root, "t") };
}

function values(row: StoredRow | null): Value[] {
  if (row === null) return [];
  return Array.from({ length: row.length }, (_, column) => row.value(column));
}

/** Counts the pages read through it. */
class CountingReader implements PageReader {
  reads = 0;

  constructor(private readonly pages: PageReader) {}

  get pageSize(): number {
    return this.pages.pageSize;
  }

  read(pageNo: number): Uint8Array {
    this.reads++;
    return this.pages.read(pageNo);
  }
}

describe("table rows", () => {
  it.each([1, 2])("round-trip random values (seed %i)", (seed) => {
    const { table } = setup();
    const random = mulberry32(seed);
    const rows = Array.from({ length: 300 }, () =>
      Array.from({ length: 1 + Math.floor(random() * 5) }, () =>
        randomValue(random),
      ),
    );
    for (const [rowid, row] of rows.entries()) {
      expect(table.insert(rowid, row)).toBe(true);
    }
    for (const [rowid, row] of rows.entries()) {
      // rowKey is equal exactly when every value is, and the count too.
      expect(rowKey(values(table.get(rowid)))).toBe(rowKey(row));
    }
  });

  it("moves the largest values to overflow pages until the row fits", () => {
    const { pages, table } = setup();
    const big = "x".repeat(10_000);
    const blob = new Uint8Array(5000).fill(7);
    const json = { items: Array.from({ length: 300 }, (_, index) => index) };
    table.insert(1, ["small", big, blob, json, 42]);
    pages.commit();

    expect(values(table.get(1))).toEqual(["small", big, blob, json, 42]);
  });

  it("reads a row's small columns without touching its overflow pages", () => {
    const { pages, root, table } = setup();
    table.insert(1, ["title", "x".repeat(50_000)]);
    pages.commit();
    const counting = new CountingReader(pages);
    const row = new TableReader(counting, root).get(1);
    const readsForRow = counting.reads;

    expect(row?.value(0)).toBe("title");
    expect(counting.reads).toBe(readsForRow);
    expect(row?.value(1)).toHaveLength(50_000);
    expect(counting.reads).toBeGreaterThan(readsForRow + 10);
  });

  it("frees overflow pages when a row is replaced, deleted or dropped", () => {
    const { pages, table } = setup();
    const before = pages.usedPages.length;
    table.insert(1, ["x".repeat(20_000)]);
    pages.commit();
    expect(pages.usedPages.length).toBeGreaterThan(before + 4);

    table.replace(1, ["short"]);
    pages.commit();
    expect(pages.usedPages).toHaveLength(before);

    table.insert(2, [new Uint8Array(30_000)]);
    table.delete(2);
    pages.commit();
    expect(pages.usedPages).toHaveLength(before);

    table.insert(3, ["y".repeat(9000)]);
    table.drop();
    pages.commit();
    expect(pages.usedPages).toEqual([]);
  });

  it("refuses a taken rowid without writing anything", () => {
    const { pages, table } = setup();
    table.insert(1, ["a"]);
    pages.commit();
    const used = pages.usedPages.length;
    expect(table.insert(1, ["x".repeat(20_000)])).toBe(false);
    pages.commit();
    expect(pages.usedPages).toHaveLength(used);
  });

  it("refuses a row that does not fit even with every value in overflow pages", () => {
    const { table } = setup();
    const columns = Array.from({ length: 300 }, (_, index) => index * 1000);
    expect(() => table.insert(1, columns)).toThrow(RowTooLargeError);
    expect(() => table.insert(1, columns)).toThrow(/row of table "t"/);
  });

  it("knows its largest rowid", () => {
    const { table } = setup();
    expect(table.lastRowid()).toBeNull();
    table.insert(-5, ["a"]);
    table.insert(7, ["b"]);
    expect(table.lastRowid()).toBe(7);
    expect([...table.scan(0)].map((row) => row.rowid)).toEqual([7]);
  });
});

/** Rows 0, 2, …, 998 of (label, number), across many leaves. */
function filled(): TableWriter {
  const { pages, table } = setup();
  for (let rowid = 0; rowid < 1000; rowid += 2) {
    table.insert(rowid, [`row ${String(rowid)}`, rowid * 10]);
  }
  pages.commit();
  return table;
}

describe("table tuples", () => {
  /** (number, rowid, label, a field no record has: "new"). */
  const layout = tupleLayout([1, ROWID_FIELD, 0, 2], [null, null, null, "new"]);

  it("read rows in rowid order or its reverse, the values a layout asks for", () => {
    const table = filled();
    const up = [...table.tuples(layout, [-Infinity, Infinity], "ascending")];
    expect(up).toHaveLength(500);
    expect(up[0]).toEqual([0, 0, "row 0", "new"]);
    expect(up.at(-1)).toEqual([9980, 998, "row 998", "new"]);
    const down = [...table.tuples(layout, [-Infinity, Infinity], "descending")];
    expect(down).toEqual(up.toReversed());
  });

  it("read the rows of a rowid range either way, both ends included", () => {
    const table = filled();
    const rowids = (range: [number, number], order: RowOrder) =>
      [...table.tuples(layout, range, order)].map(([, rowid]) => rowid);
    expect(rowids([3, 9], "ascending")).toEqual([4, 6, 8]);
    expect(rowids([4, 8], "descending")).toEqual([8, 6, 4]);
    expect(rowids([3.5, 8.5], "descending")).toEqual([8, 6, 4]);
    expect(rowids([-Infinity, 2], "descending")).toEqual([2, 0]);
    expect(rowids([996, Infinity], "ascending")).toEqual([996, 998]);
    expect(rowids([9, 3], "ascending")).toEqual([]);
    expect(rowids([Infinity, Infinity], "descending")).toEqual([]);
  });

  it("read one row by rowid, values from overflow pages included", () => {
    const { table } = setup();
    const big = "x".repeat(10_000);
    table.insert(5, ["small", big]);
    expect(table.tupleAt(5, tupleLayout([1, 0], [null, null]))).toEqual([
      big,
      "small",
    ]);
    expect(table.tupleAt(6, layout)).toBeNull();
  });
});

describe("index entries", () => {
  it("keep keys in byte order and find them", () => {
    const pages = new MemoryPages(PAGE_SIZE);
    const index = new IndexWriter(pages, IndexWriter.create(pages), "i");
    const keys = Array.from({ length: 2000 }, (_, rowid) =>
      encodeKey([`k${String(rowid % 97)}`, rowid]),
    );
    for (const key of keys) expect(index.insert(key)).toBe(true);
    expect(index.insert(keys[0] ?? new Uint8Array())).toBe(false);
    expect([...index.keysFrom()]).toEqual(keys.toSorted(compareBytes));
    expect(index.has(keys[5] ?? new Uint8Array())).toBe(true);

    // The entries for "k3" are those of rowids 3, 100, 197, …, in rowid order.
    const prefix = encodeKey(["k3"]);
    const end = prefixEnd(prefix);
    const matching = [...index.keysFrom(prefix)].filter(
      (key) => compareBytes(key, end) < 0,
    );
    expect(matching.map((key) => keyRowid(key))).toEqual(
      Array.from({ length: 21 }, (_, count) => 3 + 97 * count),
    );
  });

  it("take keys up to the limit, through splits, and refuse longer ones", () => {
    const pages = new MemoryPages(PAGE_SIZE);
    const index = new IndexWriter(pages, IndexWriter.create(pages), "i");
    const tooLong = new Uint8Array(index.maxKeySize + 1);
    expect(() => index.insert(tooLong)).toThrow(IndexKeyTooLargeError);

    // Four fit a leaf, so twenty make interior pages of the longest keys.
    const longest = Array.from({ length: 20 }, (_, count) =>
      new Uint8Array(index.maxKeySize).fill(count + 1),
    );
    for (const key of longest) expect(index.insert(key)).toBe(true);
    expect([...index.keysFrom()]).toEqual(longest);
  });
});
