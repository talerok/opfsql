import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { mulberry32 } from "../../../test/support/random.js";
import { OPFSSyncStorage } from "../backend/opfs-storage.js";
import { BTree, type BTreeLayout, type LeafNode, type Node } from "../btree.js";
import { Catalog } from "../catalog.js";
import { encodePage, encodedSize } from "../page-codec.js";
import type { SessionStore } from "../session-store.js";
import { Storage } from "../storage.js";
import type { SyncIPageStore } from "../types.js";

// The core is tested through entries shaped like table rows — [key, bytes] —
// so that the store's integrity check can verify every tree it builds.

type Entry = [key: number, payload: Uint8Array];

class TooLarge extends Error {
  constructor(
    readonly size: number,
    readonly limit: number,
  ) {
    super(`${size} > ${limit}`);
  }
}

const layout: BTreeLayout<number, Entry> = {
  keyOf: ([key]) => key,
  compare: (a, b) => a - b,
  tooLarge: (size, limit) => new TooLarge(size, limit),
};

const PAGE_SIZE = 4096;

let seq = 0;
let storage: Storage;
let ps: SessionStore;
let rootPageNo: number;
let tree: BTree<number, Entry>;

beforeEach(async () => {
  resetMockOPFS();
  storage = new Storage(
    new OPFSSyncStorage(`btree-${seq++}`, { pageSize: PAGE_SIZE }),
  );
  await storage.open();
  ps = storage.createSession();
  rootPageNo = BTree.create(ps);
  tree = new BTree(frozen(ps), rootPageNo, layout);
});

/** Hands out nodes frozen: the tree must replace nodes, never mutate them. */
function frozen(pages: SessionStore): SyncIPageStore {
  return {
    get pageCapacity() {
      return pages.pageCapacity;
    },
    readPage: <T>(pageNo: number) => deepFreeze(pages.readPage<T>(pageNo)),
    writePage: (pageNo, value) => pages.writePage(pageNo, value),
    allocPage: () => pages.allocPage(),
    freePage: (pageNo) => pages.freePage(pageNo),
    commit: () => pages.commit(),
    rollback: () => pages.rollback(),
  };
}

function deepFreeze<T>(value: T): T {
  if (
    typeof value === "object" &&
    value !== null &&
    !ArrayBuffer.isView(value)
  ) {
    Object.values(value).forEach(deepFreeze);
    Object.freeze(value);
  }
  return value;
}

function entry(key: number, size = 8): Entry {
  return [key, new Uint8Array(size).fill(key & 0xff)];
}

/** Commits, then runs the store's integrity check over the tree. */
function checkTree(): string[] {
  ps.commit();
  const catalog = new Catalog();
  catalog.addTable({ name: "t", columns: [], rootPageNo });
  return storage.checkIntegrity(catalog);
}

function leaves(): LeafNode<Entry>[] {
  const result: LeafNode<Entry>[] = [];
  const visit = (pageNo: number) => {
    const node = ps.readPage<Node<number, Entry>>(pageNo)!;
    if (node.kind === "leaf") result.push(node);
    else node.children.forEach(visit);
  };
  visit(rootPageNo);
  return result;
}

/** Share of the page each leaf's encoding takes. */
function fill(leaf: LeafNode<Entry>): number {
  return encodePage(leaf).bytes.length / ps.pageCapacity;
}

function keys(entries: Iterable<Entry>): number[] {
  return [...entries].map(([key]) => key);
}

describe("BTree", () => {
  it("keeps its root on the same page as it grows", () => {
    for (let key = 0; key < 2000; key++) tree.insert(entry(key));
    expect(ps.readPage<Node<number, Entry>>(rootPageNo)!.kind).toBe("internal");
    expect(keys(tree.ascending())).toEqual([...Array(2000).keys()]);
    expect(checkTree()).toEqual([]);
  });

  it("reports existing and missing keys without changing anything", () => {
    expect(tree.insert(entry(1))).toBe(true);
    expect(tree.insert(entry(1, 100))).toBe(false);
    expect(tree.replace(entry(2))).toBe(false);
    expect(tree.delete(2)).toBe(false);
    expect(tree.get(1)).toEqual(entry(1));
    expect(tree.get(2)).toBeNull();
    expect(checkTree()).toEqual([]);
  });

  it("fills pages completely on ascending inserts", () => {
    for (let key = 0; key < 1000; key++) tree.insert(entry(key, 100));
    const all = leaves();
    expect(all.length).toBeGreaterThan(20);
    for (const leaf of all.slice(0, -1))
      expect(fill(leaf)).toBeGreaterThan(0.95);
    expect(checkTree()).toEqual([]);
  });

  it("splits evenly under random inserts", () => {
    const random = mulberry32(7);
    const inserted = new Set<number>();
    while (inserted.size < 1000) {
      const key = Math.floor(random() * 1e6);
      if (tree.insert(entry(key, 100))) inserted.add(key);
    }
    const all = leaves();
    const average =
      all.reduce((total, leaf) => total + fill(leaf), 0) / all.length;
    expect(average).toBeGreaterThan(0.5);
    // The last leaf may have just been split off at the right edge.
    for (const leaf of all.slice(0, -1))
      expect(fill(leaf)).toBeGreaterThan(0.25);
    expect(checkTree()).toEqual([]);
  });

  it("splits a leaf when a replaced entry grows", () => {
    for (let key = 0; key < 30; key++) tree.insert(entry(key, 100));
    expect(leaves()).toHaveLength(1);

    tree.replace(entry(10, 1500));
    expect(leaves().length).toBeGreaterThan(1);
    expect(tree.get(10)).toEqual(entry(10, 1500));
    expect(checkTree()).toEqual([]);
  });

  it("accepts entries up to the limit it reports, and none above", () => {
    let limit = 0;
    try {
      tree.insert(entry(0, PAGE_SIZE));
    } catch (err) {
      expect(err).toBeInstanceOf(TooLarge);
      limit = (err as TooLarge).limit;
    }
    expect(limit).toBeGreaterThan(PAGE_SIZE / 2 - 100);

    // The largest payload whose entry encodes to exactly the limit.
    let size = limit;
    while (encodedSize(entry(1, size)) > limit) size--;
    expect(encodedSize(entry(1, size))).toBe(limit);
    expect(tree.insert(entry(1, size))).toBe(true);
    expect(() => tree.insert(entry(2, size + 1))).toThrow(TooLarge);

    // Two of the largest entries and a small one still split into fitting halves.
    tree.insert(entry(3, size));
    tree.insert(entry(2, 10));
    expect(keys(tree.ascending())).toEqual([1, 2, 3]);
    expect(checkTree()).toEqual([]);
  });

  it("iterates from a key in both directions, past emptied leaves", () => {
    for (let key = 0; key < 2000; key += 2) tree.insert(entry(key, 30));
    for (let key = 400; key < 1600; key += 2) tree.delete(key);

    expect(keys(tree.ascending(395)).slice(0, 3)).toEqual([396, 398, 1600]);
    expect(keys(tree.ascending(1999))).toEqual([]);
    expect(keys(tree.descending(1601)).slice(0, 3)).toEqual([1600, 398, 396]);
    expect(keys(tree.descending(0))).toEqual([]);
    expect(keys(tree.descending()).slice(0, 2)).toEqual([1998, 1996]);
    expect(tree.last()).toEqual(entry(1998, 30));
    expect(checkTree()).toEqual([]);
  });

  it("builds full pages from sorted entries", () => {
    const entries = Array.from({ length: 3000 }, (_, key) => entry(key, 50));
    tree.build(entries);

    expect(keys(tree.ascending())).toEqual(keys(entries));
    for (const leaf of leaves().slice(0, -1)) {
      expect(fill(leaf)).toBeGreaterThan(0.95);
    }
    expect(checkTree()).toEqual([]);

    tree.insert(entry(3000));
    tree.delete(1500);
    expect(checkTree()).toEqual([]);
  });

  it("frees every page on drop", () => {
    for (let key = 0; key < 2000; key++) tree.insert(entry(key, 20));
    ps.commit();
    tree.drop();
    ps.commit();
    expect(storage.checkIntegrity(new Catalog())).toEqual([]);
  });

  it.each([1, 2, 3, 4, 5])(
    "matches a sorted map under random changes (seed %i)",
    (seed) => {
      const random = mulberry32(seed);
      const model = new Map<number, Entry>();
      const size = () =>
        random() < 0.05
          ? Math.floor(random() * 1800)
          : Math.floor(random() * 120);

      for (let op = 0; op < 3000; op++) {
        const roll = random();
        const key = Math.floor(random() * 1500);
        if (roll < 0.15) {
          // Ascending run at the right edge.
          const last = tree.last()?.[0] ?? 0;
          for (let k = last + 1; k <= last + 20; k++) {
            const e = entry(k, size());
            expect(tree.insert(e)).toBe(true);
            model.set(k, e);
          }
        } else if (roll < 0.55) {
          const e = entry(key, size());
          expect(tree.insert(e)).toBe(!model.has(key));
          if (!model.has(key)) model.set(key, e);
        } else if (roll < 0.7) {
          const e = entry(key, size());
          expect(tree.replace(e)).toBe(model.has(key));
          if (model.has(key)) model.set(key, e);
        } else {
          expect(tree.delete(key)).toBe(model.delete(key));
        }

        if (op % 500 === 499) {
          const sorted = [...model.keys()].sort((a, b) => a - b);
          expect(keys(tree.ascending())).toEqual(sorted);
          expect(keys(tree.descending())).toEqual([...sorted].reverse());
          const from = Math.floor(random() * 1600);
          expect(keys(tree.ascending(from))).toEqual(
            sorted.filter((k) => k >= from),
          );
          expect(keys(tree.descending(from))).toEqual(
            sorted.filter((k) => k < from).reverse(),
          );
          for (const k of [key, sorted[0], sorted.at(-1)]) {
            if (k !== undefined)
              expect(tree.get(k)).toEqual(model.get(k) ?? null);
          }
          expect(checkTree()).toEqual([]);
        }
      }
    },
  );
});
