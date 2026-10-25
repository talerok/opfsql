import { describe, expect, it } from "vitest";
import { mulberry32 } from "../../../../test/support/random.js";
import { compareBytes } from "../../../values/index.js";
import {
  BTreePage,
  indexLeafCell,
  tableLeafCell,
} from "../../format/btree-page.js";
import { encodeKey } from "../../format/key.js";
import type { BTree } from "../btree.js";
import { BTreeWriter } from "../btree.js";
import { checkTree } from "../check.js";
import type { TreeKind } from "../types.js";
import { MemoryPages } from "./memory-pages.js";

const PAGE_SIZE = 4096;

function setup(kind: TreeKind = "table") {
  const pages = new MemoryPages(PAGE_SIZE);
  const root = BTreeWriter.create(pages, kind);
  pages.commit();
  return { pages, root, tree: new BTreeWriter(pages, root, kind) };
}

function record(rowid: number, size: number): Uint8Array {
  return new Uint8Array(size).fill(rowid % 251);
}

function rowids(tree: BTree, from?: number): number[] {
  return [...tree.ascending(from)].map(({ page, index }) => page.rowid(index));
}

function fill(page: BTreePage): number {
  return 1 - page.freeSpace / PAGE_SIZE;
}

function leaves(pages: MemoryPages, root: number): BTreePage[] {
  const result: BTreePage[] = [];
  const visit = (pageNo: number): void => {
    const page = new BTreePage(pages.read(pageNo));
    if (page.isLeaf) {
      result.push(page);
      return;
    }
    for (let index = 0; index <= page.cellCount; index++) {
      visit(page.child(index));
    }
  };
  visit(root);
  return result;
}

function insertAscending(tree: BTreeWriter, count: number, size: number): void {
  for (let rowid = 1; rowid <= count; rowid++) {
    tree.insert(rowid, tableLeafCell(rowid, record(rowid, size)));
  }
}

/** `count` different integers below `limit`. */
function distinctRandom(
  random: () => number,
  count: number,
  limit: number,
): number[] {
  const values = new Set<number>();
  while (values.size < count) values.add(Math.floor(random() * limit));
  return [...values];
}

describe("BTree", () => {
  it("keeps its root on the same page as it grows", () => {
    const { pages, root, tree } = setup();
    insertAscending(tree, 3000, 20);
    pages.commit();
    expect(new BTreePage(pages.read(root)).isLeaf).toBe(false);
    expect(rowids(tree)).toEqual(
      Array.from({ length: 3000 }, (_, index) => index + 1),
    );
    expect(checkTree(pages, root, "table").problems).toEqual([]);
  });

  it("fills pages completely on ascending inserts", () => {
    const { pages, root, tree } = setup();
    insertAscending(tree, 2000, 100);
    pages.commit();
    const all = leaves(pages, root);
    expect(all.length).toBeGreaterThan(40);
    const fullLeaves = all.slice(0, -1);
    for (const leaf of fullLeaves) expect(fill(leaf)).toBeGreaterThan(0.95);
  });

  it("splits evenly under random inserts", () => {
    const { pages, root, tree } = setup();
    const randomRowids = distinctRandom(mulberry32(5), 2000, 1e6);
    for (const rowid of randomRowids) {
      tree.insert(rowid, tableLeafCell(rowid, record(rowid, 100)));
    }
    pages.commit();
    const all = leaves(pages, root);
    const average =
      all.reduce((total, leaf) => total + fill(leaf), 0) / all.length;
    expect(average).toBeGreaterThan(0.6);
    // The last leaf may have just been split off at the right edge.
    const settledLeaves = all.slice(0, -1);
    for (const leaf of settledLeaves) expect(fill(leaf)).toBeGreaterThan(0.4);
    expect(checkTree(pages, root, "table").problems).toEqual([]);
  });

  it("never modifies committed pages before it commits", () => {
    const { pages, tree } = setup();
    insertAscending(tree, 500, 50);
    pages.commit();
    const before = pages.committedFingerprint();
    for (let rowid = 501; rowid <= 1000; rowid++) {
      tree.insert(rowid, tableLeafCell(rowid, record(rowid, 50)));
    }
    tree.delete(17);
    tree.replace(18, tableLeafCell(18, record(18, 900)));
    expect(pages.committedFingerprint()).toBe(before);
  });

  it("iterates from a key in both directions, past emptied leaves", () => {
    const { pages, root, tree } = setup();
    for (let rowid = 0; rowid < 2000; rowid += 2) {
      tree.insert(rowid, tableLeafCell(rowid, record(rowid, 30)));
    }
    for (let rowid = 400; rowid < 1600; rowid += 2) tree.delete(rowid);

    expect(rowids(tree, 395).slice(0, 3)).toEqual([396, 398, 1600]);
    const below = [...tree.descending(1601)].map(({ page, index }) =>
      page.rowid(index),
    );
    expect(below.slice(0, 3)).toEqual([1600, 398, 396]);
    expect([...tree.descending(0)]).toEqual([]);
    const last = tree.last();
    expect(last?.page.rowid(last.index)).toBe(1998);
    pages.commit();
    expect(checkTree(pages, root, "table").problems).toEqual([]);
  });

  it("orders index keys by their bytes", () => {
    const { pages, root, tree } = setup("index");
    const random = mulberry32(3);
    const keys = Array.from({ length: 3000 }, (_, rowid) =>
      encodeKey([`name ${String(Math.floor(random() * 500))}`, rowid]),
    );
    for (const key of keys)
      expect(tree.insert(key, indexLeafCell(key))).toBe(true);
    pages.commit();
    const stored = [...tree.ascending()].map(({ page, index }) =>
      Uint8Array.from(page.key(index)),
    );
    expect(stored).toEqual(keys.toSorted(compareBytes));
    expect(checkTree(pages, root, "index").problems).toEqual([]);
  });

  it("frees every page on drop", () => {
    const { pages, root, tree } = setup();
    insertAscending(tree, 2000, 40);
    pages.commit();
    expect(
      checkTree(pages, root, "table").pages.toSorted((a, b) => a - b),
    ).toEqual(pages.usedPages);
    tree.drop();
    pages.commit();
    expect(pages.usedPages).toEqual([]);
  });

  it.each([1, 2, 3, 4])(
    "matches a sorted map under random changes (seed %i)",
    (seed) => {
      const { pages, root, tree } = setup();
      const random = mulberry32(seed);
      const model = new Map<number, number>();
      for (let step = 0; step < 4000; step++) {
        applyRandomChange(tree, model, random);
        if (step % 500 !== 499) continue;
        pages.commit();
        expectSameContent(tree, model);
        const report = checkTree(pages, root, "table");
        expect(report.problems).toEqual([]);
        expect(report.entries).toBe(model.size);
      }
    },
  );
});

/** Inserts, replaces or deletes a random rowid, in the tree and the model. */
function applyRandomChange(
  tree: BTreeWriter,
  model: Map<number, number>,
  random: () => number,
): void {
  const rowid = Math.floor(random() * 2000) - 200;
  const size =
    random() < 0.05 ? Math.floor(random() * 900) : Math.floor(random() * 80);
  const cell = tableLeafCell(rowid, record(rowid, size));
  const roll = random();
  if (roll < 0.5) {
    expect(tree.insert(rowid, cell)).toBe(!model.has(rowid));
    if (!model.has(rowid)) model.set(rowid, size);
  } else if (roll < 0.7) {
    expect(tree.replace(rowid, cell)).toBe(model.has(rowid));
    if (model.has(rowid)) model.set(rowid, size);
  } else {
    expect(tree.delete(rowid)).toBe(model.delete(rowid));
  }
}

function expectSameContent(tree: BTree, model: Map<number, number>): void {
  const sorted = [...model.keys()].toSorted((a, b) => a - b);
  expect(rowids(tree)).toEqual(sorted);
  for (const rowid of sorted.slice(0, 20)) {
    const found = tree.find(rowid);
    expect(found?.page.record(found.index)).toHaveLength(
      model.get(rowid) ?? -1,
    );
  }
}
