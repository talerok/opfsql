import { describe, expect, it } from "vitest";
import { InternalError } from "../../../errors.js";
import { tableLeafCell } from "../../format/btree-page.js";
import { TableWriter } from "../../rows/table-store.js";
import { BTreeWriter } from "../btree.js";
import { MemoryPages } from "./memory-pages.js";

// What is read from a tree is current only until that tree changes
// (docs/layers.md): a cursor or a row resumed after a change to its own tree
// fails, while a change to another tree leaves it be.

const PAGE_SIZE = 4096;

function twoTrees() {
  const pages = new MemoryPages(PAGE_SIZE);
  const first = new BTreeWriter(
    pages,
    BTreeWriter.create(pages, "table"),
    "table",
  );
  const second = new BTreeWriter(
    pages,
    BTreeWriter.create(pages, "table"),
    "table",
  );
  for (let rowid = 1; rowid <= 3; rowid++) {
    first.insert(rowid, tableLeafCell(rowid, new Uint8Array([rowid])));
  }
  return { first, second };
}

const cell = (rowid: number): Uint8Array =>
  tableLeafCell(rowid, new Uint8Array([rowid]));

describe("reading a tree while it changes", () => {
  it("fails a cursor resumed after its own tree changed", () => {
    const { first } = twoTrees();
    const cursor = first.ascending();
    cursor.next();
    first.insert(10, cell(10));
    expect(() => cursor.next()).toThrow(InternalError);
  });

  it("lets a cursor go on when another tree changes", () => {
    const { first, second } = twoTrees();
    const cursor = first.ascending();
    cursor.next();
    second.insert(10, cell(10));
    const rest = [...{ [Symbol.iterator]: () => cursor }];
    expect(rest.map(({ page, index }) => page.rowid(index))).toEqual([2, 3]);
  });

  it("does not count an insert that changed nothing", () => {
    const { first } = twoTrees();
    const cursor = first.ascending();
    cursor.next();
    expect(first.insert(1, cell(1))).toBe(false);
    expect(cursor.next().done).toBe(false);
  });

  it("fails a row's overflow value read after its table changed", () => {
    const pages = new MemoryPages(PAGE_SIZE);
    const table = new TableWriter(pages, TableWriter.create(pages), "t");
    const big = "x".repeat(10_000);
    table.insert(1, [big]);
    table.insert(2, ["small"]);
    const row = table.get(1);
    if (row === null) throw new Error("row 1 is missing");

    table.replace(2, ["changed"]);

    expect(() => row.value(0)).toThrow(InternalError);
    expect(table.get(1)?.value(0)).toBe(big);
  });
});
