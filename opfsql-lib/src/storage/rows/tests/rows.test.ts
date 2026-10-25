import { describe, expect, it } from "vitest";
import { mulberry32 } from "../../../../test/support/random.js";
import { randomValue } from "../../../../test/support/values.js";
import { IndexKeyTooLargeError, RowTooLargeError } from "../../../errors.js";
import type { Value } from "../../../values/index.js";
import { areEqual, compareBytes } from "../../../values/index.js";
import type { PageReader } from "../../btree/page-store.js";
import { MemoryPages } from "../../btree/tests/memory-pages.js";
import { encodeKey } from "../../format/key.js";
import { IndexWriter } from "../index-store.js";
import type { StoredRow } from "../table-store.js";
import { TableReader, TableWriter } from "../table-store.js";

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
      const stored = values(table.get(rowid));
      expect(
        stored.every((value, column) => areEqual(value, row[column] ?? null)),
      ).toBe(true);
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

    const prefix = encodeKey(["k3"]);
    const matching = [...index.keysFrom(prefix)].filter(
      (key) => compareBytes(key.subarray(0, prefix.length), prefix) === 0,
    );
    expect(matching.length).toBeGreaterThan(0);
  });

  it("refuse a key longer than a quarter page", () => {
    const pages = new MemoryPages(PAGE_SIZE);
    const index = new IndexWriter(pages, IndexWriter.create(pages), "i");
    const tooLong = encodeKey(["x".repeat(index.maxKeySize)]);
    expect(() => index.insert(tooLong)).toThrow(IndexKeyTooLargeError);
    expect(index.insert(encodeKey(["x".repeat(index.maxKeySize - 20)]))).toBe(
      true,
    );
  });
});
