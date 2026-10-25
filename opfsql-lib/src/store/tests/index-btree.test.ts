import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { BTree, type InternalNode } from "../btree.js";
import { IndexKeyTooLargeError } from "../errors.js";
import { SyncBTree } from "../index-btree/index-btree.js";
import type { IndexKey } from "../index-btree/types.js";
import { Storage } from "../storage.js";
import type { SessionStore } from "../session-store.js";
import type { RowId, SyncIPageStore } from "../types.js";
import { OPFSSyncStorage } from "../backend/opfs-storage.js";

let seq = 0;

function rid(a: number, b: number): RowId {
  return a * 1000 + b;
}

/** Small pages, so that a few thousand keys split nodes. */
async function createStore(): Promise<SessionStore> {
  const s = new OPFSSyncStorage(`ibt-test-${seq++}`, { pageSize: 4096 });
  const storage = new Storage(s);
  await storage.open();
  return storage.createSession();
}

let rootPageNo: number;

/** Create an empty index tree. */
function createTree(ps: SyncIPageStore, unique = false): SyncBTree {
  rootPageNo = BTree.create(ps);
  return new SyncBTree(ps, rootPageNo, { name: "idx", unique });
}

function root(ps: SyncIPageStore) {
  return ps.readPage<InternalNode<IndexKey>>(rootPageNo)!;
}

describe("SyncBTree", () => {
  let ps: SessionStore;
  let tree: SyncBTree;

  beforeEach(async () => {
    resetMockOPFS();
    ps = await createStore();
    tree = createTree(ps, false);
  });

  describe("insert and lookup", () => {
    it("insert single key and find it", () => {
      tree.insert([1], rid(0, 0));
      ps.commit();

      expect(tree.lookup([1])).toEqual([rid(0, 0)]);
    });

    it("insert multiple keys and find each", () => {
      tree.insert([1], rid(0, 0));
      tree.insert([2], rid(0, 1));
      tree.insert([3], rid(0, 2));
      ps.commit();

      for (let i = 1; i <= 3; i++) {
        expect(tree.lookup([i])).toEqual([rid(0, i - 1)]);
      }
    });

    it("lookup non-existent key returns empty", () => {
      tree.insert([1], rid(0, 0));
      ps.commit();
      expect(tree.lookup([999])).toEqual([]);
    });

    it("lookup on empty tree returns empty", () => {
      expect(tree.lookup([1])).toEqual([]);
    });

    it("duplicate keys in non-unique index accumulate rowIds", () => {
      tree.insert([5], rid(0, 0));
      tree.insert([5], rid(0, 1));
      tree.insert([5], rid(1, 0));
      ps.commit();

      const results = tree.lookup([5]);
      expect(results).toHaveLength(3);
      expect(results).toContainEqual(rid(0, 0));
      expect(results).toContainEqual(rid(0, 1));
      expect(results).toContainEqual(rid(1, 0));
    });

    it("keys are stored in sorted order", () => {
      tree.insert([30], rid(0, 2));
      tree.insert([10], rid(0, 0));
      tree.insert([20], rid(0, 1));
      ps.commit();

      const results = tree.range({ lower: [1] });
      expect(results).toEqual([rid(0, 0), rid(0, 1), rid(0, 2)]);
    });
  });

  describe("delete", () => {
    it("delete existing key", () => {
      tree.insert([1], rid(0, 0));
      tree.insert([2], rid(0, 1));
      ps.commit();

      tree.delete([1], rid(0, 0));
      ps.commit();

      expect(tree.lookup([1])).toEqual([]);
      expect(tree.lookup([2])).toEqual([rid(0, 1)]);
    });

    it("delete non-existent key is a no-op", () => {
      tree.insert([1], rid(0, 0));
      ps.commit();

      tree.delete([999], rid(0, 0));
      ps.commit();

      expect(tree.lookup([1])).toEqual([rid(0, 0)]);
    });

    it("delete one rowId from duplicate key preserves others", () => {
      tree.insert([5], rid(0, 0));
      tree.insert([5], rid(0, 1));
      ps.commit();

      tree.delete([5], rid(0, 0));
      ps.commit();

      expect(tree.lookup([5])).toEqual([rid(0, 1)]);
    });

    it("delete on empty tree is a no-op", () => {
      tree.delete([1], rid(0, 0));
    });

    it("delete wrong rowId for existing key is a no-op", () => {
      tree.insert([1], rid(0, 0));
      ps.commit();

      tree.delete([1], rid(9, 9));
      ps.commit();

      expect(tree.lookup([1])).toEqual([rid(0, 0)]);
    });
  });

  describe("range scans", () => {
    beforeEach(() => {
      for (let i = 1; i <= 10; i++) tree.insert([i * 10], rid(0, i - 1));
      ps.commit();
    });

    it("GREATER", () => {
      expect(tree.range({ lower: [80], lowerInclusive: false })).toEqual([
        rid(0, 8),
        rid(0, 9),
      ]);
    });

    it("GREATER_EQUAL", () => {
      expect(tree.range({ lower: [90] })).toEqual([rid(0, 8), rid(0, 9)]);
    });

    it("LESS", () => {
      expect(tree.range({ upper: [30], upperInclusive: false })).toEqual([
        rid(0, 0),
        rid(0, 1),
      ]);
    });

    it("LESS_EQUAL", () => {
      expect(tree.range({ upper: [20] })).toEqual([rid(0, 0), rid(0, 1)]);
    });

    it("bounded range (GREATER_EQUAL + LESS)", () => {
      expect(
        tree.range({ lower: [30], upper: [60], upperInclusive: false }),
      ).toEqual([rid(0, 2), rid(0, 3), rid(0, 4)]);
    });
  });

  describe("composite keys", () => {
    it("lookup composite equality", () => {
      tree.insert(["a", 1], rid(0, 0));
      tree.insert(["a", 2], rid(0, 1));
      tree.insert(["b", 1], rid(0, 2));
      ps.commit();

      expect(tree.lookup(["a", 2])).toEqual([rid(0, 1)]);
    });

    it("prefix scan on composite index", () => {
      tree.insert(["a", 1], rid(0, 0));
      tree.insert(["a", 2], rid(0, 1));
      tree.insert(["b", 1], rid(0, 2));
      ps.commit();

      const results = tree.range({ lower: ["a"], upper: ["a"] });
      expect(results).toHaveLength(2);
      expect(results).toContainEqual(rid(0, 0));
      expect(results).toContainEqual(rid(0, 1));
    });
  });

  describe("unique constraint", () => {
    let uniqueTree: SyncBTree;

    beforeEach(() => {
      uniqueTree = createTree(ps, true);
    });

    it("throws on duplicate insert", () => {
      uniqueTree.insert([1], rid(0, 0));
      expect(() => uniqueTree.insert([1], rid(0, 1))).toThrow(
        "UNIQUE constraint failed",
      );
    });

    it("allows duplicate NULL in unique index", () => {
      uniqueTree.insert([null], rid(0, 0));
      uniqueTree.insert([null], rid(0, 1));
      ps.commit();

      expect(uniqueTree.lookup([null])).toHaveLength(2);
    });

    it("allows NULL in composite key even if other column matches", () => {
      uniqueTree.insert([1, null], rid(0, 0));
      uniqueTree.insert([1, null], rid(0, 1));
      ps.commit();
    });

    it("different keys are allowed in unique index", () => {
      uniqueTree.insert([1], rid(0, 0));
      uniqueTree.insert([2], rid(0, 1));
      ps.commit();

      expect(uniqueTree.lookup([1])).toEqual([rid(0, 0)]);
    });

    it("detects duplicate after leaf split (boundary case)", () => {
      // A leaf split promotes a separator whose first column equals an
      // existing user key, at the start of the right leaf. Looking up the
      // user key descends into the left leaf and must go on to the right one.
      for (let i = 0; i < 2000; i++) uniqueTree.insert([i], rid(0, i));
      ps.commit();

      const [boundary] = root(ps).keys[0];
      expect(() => uniqueTree.insert([boundary], rid(0, 9999))).toThrow(
        "UNIQUE constraint failed",
      );
    });
  });

  describe("NULL keys", () => {
    it("NULL sorts last (after all non-null values)", () => {
      tree.insert([null], rid(0, 2));
      tree.insert([1], rid(0, 0));
      tree.insert([100], rid(0, 1));
      ps.commit();

      const results = tree.range({ lower: [1] });
      expect(results).toEqual([rid(0, 0), rid(0, 1), rid(0, 2)]);
    });

    it("exact lookup for NULL finds it", () => {
      tree.insert([null], rid(0, 0));
      tree.insert([1], rid(0, 1));
      ps.commit();

      expect(tree.lookup([null])).toEqual([rid(0, 0)]);
    });
  });

  describe("node splitting", () => {
    it("handles enough inserts to trigger leaf split", () => {
      for (let i = 0; i < 3000; i++)
        tree.insert([i], rid(Math.floor(i / 50), i % 50));
      ps.commit();

      expect(root(ps).kind).toBe("internal");
      for (let i = 0; i < 3000; i++) {
        expect(tree.lookup([i])).toHaveLength(1);
      }
    });

    it("range scan works across split nodes", () => {
      for (let i = 0; i < 3000; i++) tree.insert([i], rid(0, i));
      ps.commit();

      const [boundary] = root(ps).keys[0] as number[];
      expect(
        tree.range({ lower: [boundary - 5], upper: [boundary + 4] }),
      ).toHaveLength(10);
    });

    it("handles reverse-order inserts (worst case for splits)", () => {
      for (let i = 3000; i >= 0; i--) tree.insert([i], rid(0, i));
      ps.commit();

      expect(root(ps).kind).toBe("internal");
      for (const k of [0, 750, 1500, 2250, 3000]) {
        expect(tree.lookup([k])).toEqual([rid(0, k)]);
      }
    });

    it("non-unique index with many duplicates splits without page overflow", () => {
      // Each (key, rowId) is a separate entry, so leaves split however many
      // rows share one indexed value.
      for (let i = 0; i < 3000; i++) tree.insert([42], rid(0, i));
      ps.commit();

      expect(root(ps).kind).toBe("internal");
      const results = tree.lookup([42]);
      expect(results).toHaveLength(3000);
      expect(new Set(results).size).toBe(3000);
    });

    it("rejects a key over a quarter of a page, changing nothing", () => {
      tree.insert(["small"], rid(0, 0));
      expect(() => tree.insert(["x".repeat(1100)], rid(0, 1))).toThrow(
        IndexKeyTooLargeError,
      );
      expect(() => tree.insert(["x".repeat(1100)], rid(0, 1))).toThrow(
        /key of index "idx" is \d+ bytes, exceeds the limit of \d+ bytes/,
      );
      tree.insert(["x".repeat(900)], rid(0, 2));
      expect(tree.range()).toEqual([rid(0, 0), rid(0, 2)]);
    });
  });

  describe("first and last", () => {
    it("skip entries whose first column is NULL", () => {
      tree.insert([null, 1], rid(0, 0));
      tree.insert([5, 1], rid(0, 1));
      tree.insert([7, null], rid(0, 2));
      tree.insert([3, null], rid(0, 3));

      expect(tree.first()).toEqual({ key: [3, null], rowId: rid(0, 3) });
      expect(tree.last()).toEqual({ key: [7, null], rowId: rid(0, 2) });
    });

    it("are null when every first column is NULL", () => {
      tree.insert([null], rid(0, 0));
      expect(tree.first()).toBeNull();
      expect(tree.last()).toBeNull();
    });

    it("look past leaves emptied by deletes", () => {
      for (let i = 0; i < 3000; i++) tree.insert([i], rid(0, i));
      for (let i = 100; i < 3000; i++) tree.delete([i], rid(0, i));
      for (let i = 0; i < 50; i++) tree.delete([i], rid(0, i));

      expect(tree.first()).toEqual({ key: [50], rowId: rid(0, 50) });
      expect(tree.last()).toEqual({ key: [99], rowId: rid(0, 99) });
    });
  });

  describe("bulkLoad", () => {
    it("bulk load sorted entries", () => {
      const bt = new SyncBTree(ps, BTree.create(ps), {
        name: "idx",
        unique: false,
      });
      const entries = Array.from({ length: 50 }, (_, i) => ({
        key: [i] as IndexKey,
        rowId: rid(0, i),
      }));
      bt.bulkLoad(entries);
      ps.commit();

      for (let i = 0; i < 50; i++) {
        expect(bt.lookup([i])).toEqual([rid(0, i)]);
      }
    });

    it("bulk load empty entries creates empty tree", () => {
      const bt = new SyncBTree(ps, BTree.create(ps), {
        name: "idx",
        unique: false,
      });
      bt.bulkLoad([]);
      ps.commit();
      expect(bt.lookup([1])).toEqual([]);
    });

    it("bulk load stores duplicate keys as separate entries in non-unique index", () => {
      const bt = new SyncBTree(ps, BTree.create(ps), {
        name: "idx",
        unique: false,
      });
      const entries = [
        { key: [1] as IndexKey, rowId: rid(0, 0) },
        { key: [1] as IndexKey, rowId: rid(0, 1) },
        { key: [2] as IndexKey, rowId: rid(0, 2) },
      ];
      bt.bulkLoad(entries);
      ps.commit();

      expect(bt.lookup([1])).toHaveLength(2);
    });

    it("bulk load unique rejects duplicates", () => {
      const bt = new SyncBTree(ps, BTree.create(ps), {
        name: "idx",
        unique: true,
      });
      const entries = [
        { key: [1] as IndexKey, rowId: rid(0, 0) },
        { key: [1] as IndexKey, rowId: rid(0, 1) },
      ];
      expect(() => bt.bulkLoad(entries)).toThrow("UNIQUE constraint failed");
    });

    it("bulk load with many entries triggers internal nodes", () => {
      rootPageNo = BTree.create(ps);
      const bt = new SyncBTree(ps, rootPageNo, { name: "idx", unique: false });
      const entries = Array.from({ length: 5000 }, (_, i) => ({
        key: [i] as IndexKey,
        rowId: rid(Math.floor(i / 100), i % 100),
      }));
      bt.bulkLoad(entries);
      ps.commit();

      expect(root(ps).kind).toBe("internal");
      for (const k of [0, 99, 2500, 4999]) {
        expect(bt.lookup([k])).toHaveLength(1);
      }
      expect(bt.range({ lower: [4990] })).toHaveLength(10);
    });
  });

  describe("missing root", () => {
    it("throws a descriptive error", () => {
      const badTree = new SyncBTree(ps, 999, { name: "idx", unique: false });
      expect(() => badTree.lookup([1])).toThrow(/page 999/);
    });
  });

  describe("drop", () => {
    it("drop removes all btree data", () => {
      tree.insert([1], rid(0, 0));
      tree.insert([2], rid(0, 1));
      ps.commit();

      tree.drop();
      ps.commit();
    });
  });

  describe("string keys", () => {
    it("string keys sort lexicographically", () => {
      tree.insert(["banana"], rid(0, 0));
      tree.insert(["apple"], rid(0, 1));
      tree.insert(["cherry"], rid(0, 2));
      ps.commit();

      const results = tree.range({ lower: ["a"], upper: ["b"] });
      expect(results).toEqual([rid(0, 1)]);
    });
  });

  describe("persistence across commits", () => {
    it("data survives commit and is readable from storage", () => {
      tree.insert([1], rid(0, 0));
      ps.commit();

      tree.insert([2], rid(0, 1));
      ps.commit();

      expect(tree.lookup([1])).toEqual([rid(0, 0)]);
      expect(tree.lookup([2])).toEqual([rid(0, 1)]);
    });
  });
});
