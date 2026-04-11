import { beforeEach, describe, expect, it } from "vitest";
import { BTree } from "../btree/btree.js";
import type { IndexKey } from "../btree/types.js";
import { PageManager } from "../page-manager.js";
import type { RowId } from "../types.js";
import { MemoryStorage } from "./memory-storage.js";

function rid(a: number, b: number): RowId {
  return a * 1000 + b;
}

describe("BTree", () => {
  let pm: PageManager;
  let tree: BTree;

  beforeEach(() => {
    pm = new PageManager(new MemoryStorage());
    tree = new BTree("test_idx", pm, false);
  });

  // -------------------------------------------------------------------------
  // Insert + Search basics
  // -------------------------------------------------------------------------

  describe("insert and search", () => {
    it("insert single key and find it", async () => {
      await tree.insert([1], rid(0, 0));
      await pm.commit();

      const results = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(results).toEqual([rid(0, 0)]);
    });

    it("insert multiple keys and find each", async () => {
      await tree.insert([1], rid(0, 0));
      await tree.insert([2], rid(0, 1));
      await tree.insert([3], rid(0, 2));
      await pm.commit();

      for (let i = 1; i <= 3; i++) {
        const results = await tree.search([
          { columnPosition: 0, comparisonType: "EQUAL", value: i },
        ]);
        expect(results).toEqual([rid(0, i - 1)]);
      }
    });

    it("search for non-existent key returns empty", async () => {
      await tree.insert([1], rid(0, 0));
      await pm.commit();

      const results = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 999 },
      ]);
      expect(results).toEqual([]);
    });

    it("search on empty tree returns empty", async () => {
      const results = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(results).toEqual([]);
    });

    it("duplicate keys in non-unique index accumulate rowIds", async () => {
      await tree.insert([5], rid(0, 0));
      await tree.insert([5], rid(0, 1));
      await tree.insert([5], rid(1, 0));
      await pm.commit();

      const results = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 5 },
      ]);
      expect(results).toHaveLength(3);
      expect(results).toContainEqual(rid(0, 0));
      expect(results).toContainEqual(rid(0, 1));
      expect(results).toContainEqual(rid(1, 0));
    });

    it("keys are stored in sorted order", async () => {
      await tree.insert([30], rid(0, 2));
      await tree.insert([10], rid(0, 0));
      await tree.insert([20], rid(0, 1));
      await pm.commit();

      // Range scan should return in order
      const results = await tree.search([
        { columnPosition: 0, comparisonType: "GREATER_EQUAL", value: 1 },
      ]);
      expect(results).toEqual([rid(0, 0), rid(0, 1), rid(0, 2)]);
    });
  });

  // -------------------------------------------------------------------------
  // Delete
  // -------------------------------------------------------------------------

  describe("delete", () => {
    it("delete existing key", async () => {
      await tree.insert([1], rid(0, 0));
      await tree.insert([2], rid(0, 1));
      await pm.commit();

      await tree.delete([1], rid(0, 0));
      await pm.commit();

      const r1 = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(r1).toEqual([]);

      const r2 = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 2 },
      ]);
      expect(r2).toEqual([rid(0, 1)]);
    });

    it("delete non-existent key is a no-op", async () => {
      await tree.insert([1], rid(0, 0));
      await pm.commit();

      await tree.delete([999], rid(0, 0));
      await pm.commit();

      const results = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(results).toEqual([rid(0, 0)]);
    });

    it("delete one rowId from duplicate key preserves others", async () => {
      await tree.insert([5], rid(0, 0));
      await tree.insert([5], rid(0, 1));
      await pm.commit();

      await tree.delete([5], rid(0, 0));
      await pm.commit();

      const results = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 5 },
      ]);
      expect(results).toEqual([rid(0, 1)]);
    });

    it("delete on empty tree is a no-op", async () => {
      await tree.delete([1], rid(0, 0));
      // No error thrown
    });

    it("delete wrong rowId for existing key is a no-op", async () => {
      await tree.insert([1], rid(0, 0));
      await pm.commit();

      await tree.delete([1], rid(9, 9));
      await pm.commit();

      const results = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(results).toEqual([rid(0, 0)]);
    });
  });

  // -------------------------------------------------------------------------
  // Range scans
  // -------------------------------------------------------------------------

  describe("range scans", () => {
    beforeEach(async () => {
      for (let i = 1; i <= 10; i++) {
        await tree.insert([i * 10], rid(0, i - 1));
      }
      await pm.commit();
    });

    it("GREATER", async () => {
      const results = await tree.search([
        { columnPosition: 0, comparisonType: "GREATER", value: 80 },
      ]);
      expect(results).toEqual([rid(0, 8), rid(0, 9)]);
    });

    it("GREATER_EQUAL", async () => {
      const results = await tree.search([
        { columnPosition: 0, comparisonType: "GREATER_EQUAL", value: 90 },
      ]);
      expect(results).toEqual([rid(0, 8), rid(0, 9)]);
    });

    it("LESS", async () => {
      const results = await tree.search([
        { columnPosition: 0, comparisonType: "LESS", value: 30 },
      ]);
      expect(results).toEqual([rid(0, 0), rid(0, 1)]);
    });

    it("LESS_EQUAL", async () => {
      const results = await tree.search([
        { columnPosition: 0, comparisonType: "LESS_EQUAL", value: 20 },
      ]);
      expect(results).toEqual([rid(0, 0), rid(0, 1)]);
    });

    it("bounded range (GREATER_EQUAL + LESS)", async () => {
      const results = await tree.search([
        { columnPosition: 0, comparisonType: "GREATER_EQUAL", value: 30 },
        { columnPosition: 0, comparisonType: "LESS", value: 60 },
      ]);
      expect(results).toEqual([rid(0, 2), rid(0, 3), rid(0, 4)]);
    });
  });

  // -------------------------------------------------------------------------
  // Composite keys
  // -------------------------------------------------------------------------

  describe("composite keys", () => {
    it("search composite equality", async () => {
      await tree.insert(["a", 1], rid(0, 0));
      await tree.insert(["a", 2], rid(0, 1));
      await tree.insert(["b", 1], rid(0, 2));
      await pm.commit();

      const results = await tree.search(
        [
          { columnPosition: 0, comparisonType: "EQUAL", value: "a" },
          { columnPosition: 1, comparisonType: "EQUAL", value: 2 },
        ],
        2,
      );
      expect(results).toEqual([rid(0, 1)]);
    });

    it("prefix scan on composite index", async () => {
      await tree.insert(["a", 1], rid(0, 0));
      await tree.insert(["a", 2], rid(0, 1));
      await tree.insert(["b", 1], rid(0, 2));
      await pm.commit();

      const results = await tree.search(
        [{ columnPosition: 0, comparisonType: "EQUAL", value: "a" }],
        2,
      );
      expect(results).toHaveLength(2);
      expect(results).toContainEqual(rid(0, 0));
      expect(results).toContainEqual(rid(0, 1));
    });
  });

  // -------------------------------------------------------------------------
  // UNIQUE constraint
  // -------------------------------------------------------------------------

  describe("unique constraint", () => {
    let uniqueTree: BTree;

    beforeEach(() => {
      uniqueTree = new BTree("unique_idx", pm, true);
    });

    it("throws on duplicate insert", async () => {
      await uniqueTree.insert([1], rid(0, 0));

      await expect(uniqueTree.insert([1], rid(0, 1))).rejects.toThrow(
        "UNIQUE constraint failed",
      );
    });

    it("allows duplicate NULL in unique index", async () => {
      await uniqueTree.insert([null], rid(0, 0));
      await uniqueTree.insert([null], rid(0, 1));
      // No error — SQL allows multiple NULLs in unique index

      await pm.commit();
      const results = await uniqueTree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: null },
      ]);
      expect(results).toHaveLength(2);
    });

    it("allows NULL in composite key even if other column matches", async () => {
      await uniqueTree.insert([1, null], rid(0, 0));
      await uniqueTree.insert([1, null], rid(0, 1));
      // No error — NULL exempts from uniqueness check
      await pm.commit();
    });

    it("different keys are allowed in unique index", async () => {
      await uniqueTree.insert([1], rid(0, 0));
      await uniqueTree.insert([2], rid(0, 1));
      await pm.commit();

      const r1 = await uniqueTree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(r1).toEqual([rid(0, 0)]);
    });
  });

  // -------------------------------------------------------------------------
  // NULL handling in search
  // -------------------------------------------------------------------------

  describe("NULL keys", () => {
    it("NULL sorts last (after all non-null values)", async () => {
      await tree.insert([null], rid(0, 2));
      await tree.insert([1], rid(0, 0));
      await tree.insert([100], rid(0, 1));
      await pm.commit();

      // In B-tree key ordering, NULL > all non-null values.
      // So GREATER_EQUAL 1 includes NULL (key [null] compares > [1]).
      // SQL NULL semantics (NULL >= 1 → unknown) are handled by executor residual filters.
      const results = await tree.search([
        { columnPosition: 0, comparisonType: "GREATER_EQUAL", value: 1 },
      ]);
      expect(results).toEqual([rid(0, 0), rid(0, 1), rid(0, 2)]);
    });

    it("exact search for NULL finds it", async () => {
      await tree.insert([null], rid(0, 0));
      await tree.insert([1], rid(0, 1));
      await pm.commit();

      const results = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: null },
      ]);
      expect(results).toEqual([rid(0, 0)]);
    });
  });

  // -------------------------------------------------------------------------
  // Splitting (many inserts to trigger node splits)
  // -------------------------------------------------------------------------

  describe("node splitting", () => {
    it("handles enough inserts to trigger leaf split", async () => {
      // ORDER = 100, so inserting 150 keys should trigger at least one split
      for (let i = 0; i < 150; i++) {
        await tree.insert([i], rid(Math.floor(i / 50), i % 50));
      }
      await pm.commit();

      // Verify all keys are findable
      for (let i = 0; i < 150; i++) {
        const results = await tree.search([
          { columnPosition: 0, comparisonType: "EQUAL", value: i },
        ]);
        expect(results).toHaveLength(1);
        expect(results[0]).toEqual(rid(Math.floor(i / 50), i % 50));
      }
    });

    it("range scan works across split nodes", async () => {
      for (let i = 0; i < 150; i++) {
        await tree.insert([i], rid(0, i));
      }
      await pm.commit();

      const results = await tree.search([
        { columnPosition: 0, comparisonType: "GREATER_EQUAL", value: 140 },
        { columnPosition: 0, comparisonType: "LESS_EQUAL", value: 149 },
      ]);
      expect(results).toHaveLength(10);
    });

    it("handles reverse-order inserts (worst case for splits)", async () => {
      for (let i = 200; i >= 0; i--) {
        await tree.insert([i], rid(0, i));
      }
      await pm.commit();

      // Verify a few keys
      for (const k of [0, 50, 100, 150, 200]) {
        const results = await tree.search([
          { columnPosition: 0, comparisonType: "EQUAL", value: k },
        ]);
        expect(results).toEqual([rid(0, k)]);
      }
    });
  });

  // -------------------------------------------------------------------------
  // Bulk load
  // -------------------------------------------------------------------------

  describe("bulkLoad", () => {
    it("bulk load sorted entries", async () => {
      const entries = Array.from({ length: 50 }, (_, i) => ({
        key: [i] as IndexKey,
        rowId: rid(0, i),
      }));
      await tree.bulkLoad(entries);
      await pm.commit();

      for (let i = 0; i < 50; i++) {
        const results = await tree.search([
          { columnPosition: 0, comparisonType: "EQUAL", value: i },
        ]);
        expect(results).toEqual([rid(0, i)]);
      }
    });

    it("bulk load empty entries creates empty tree", async () => {
      await tree.bulkLoad([]);
      await pm.commit();

      const results = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(results).toEqual([]);
    });

    it("bulk load merges duplicate keys in non-unique index", async () => {
      const entries = [
        { key: [1] as IndexKey, rowId: rid(0, 0) },
        { key: [1] as IndexKey, rowId: rid(0, 1) },
        { key: [2] as IndexKey, rowId: rid(0, 2) },
      ];
      await tree.bulkLoad(entries);
      await pm.commit();

      const results = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(results).toHaveLength(2);
    });

    it("bulk load unique rejects duplicates", async () => {
      const uniqueTree = new BTree("unique_idx", pm, true);
      const entries = [
        { key: [1] as IndexKey, rowId: rid(0, 0) },
        { key: [1] as IndexKey, rowId: rid(0, 1) },
      ];
      await expect(uniqueTree.bulkLoad(entries)).rejects.toThrow(
        "UNIQUE constraint failed",
      );
    });

    it("bulk load with many entries triggers internal nodes", async () => {
      const entries = Array.from({ length: 500 }, (_, i) => ({
        key: [i] as IndexKey,
        rowId: rid(Math.floor(i / 100), i % 100),
      }));
      await tree.bulkLoad(entries);
      await pm.commit();

      // Spot-check some keys
      for (const k of [0, 99, 250, 499]) {
        const results = await tree.search([
          { columnPosition: 0, comparisonType: "EQUAL", value: k },
        ]);
        expect(results).toHaveLength(1);
      }

      // Range scan
      const rangeResults = await tree.search([
        { columnPosition: 0, comparisonType: "GREATER_EQUAL", value: 490 },
      ]);
      expect(rangeResults).toHaveLength(10);
    });
  });

  // -------------------------------------------------------------------------
  // Drop
  // -------------------------------------------------------------------------

  describe("drop", () => {
    it("drop removes all btree data", async () => {
      await tree.insert([1], rid(0, 0));
      await tree.insert([2], rid(0, 1));
      await pm.commit();

      await tree.drop();
      await pm.commit();

      // After drop, search should find nothing (tree is gone)
      const results = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(results).toEqual([]);
    });
  });

  // -------------------------------------------------------------------------
  // String keys
  // -------------------------------------------------------------------------

  describe("string keys", () => {
    it("string keys sort lexicographically", async () => {
      await tree.insert(["banana"], rid(0, 0));
      await tree.insert(["apple"], rid(0, 1));
      await tree.insert(["cherry"], rid(0, 2));
      await pm.commit();

      const results = await tree.search([
        { columnPosition: 0, comparisonType: "GREATER_EQUAL", value: "a" },
        { columnPosition: 0, comparisonType: "LESS_EQUAL", value: "b" },
      ]);
      // Only 'apple' is in [a, b] range
      expect(results).toEqual([rid(0, 1)]);
    });
  });

  // -------------------------------------------------------------------------
  // Insert after commit (WAL cleared, read from storage)
  // -------------------------------------------------------------------------

  describe("persistence across commits", () => {
    it("data survives commit and is readable from storage", async () => {
      await tree.insert([1], rid(0, 0));
      await pm.commit();

      // After commit, WAL is cleared — reads go to storage
      await tree.insert([2], rid(0, 1));
      await pm.commit();

      const r1 = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(r1).toEqual([rid(0, 0)]);

      const r2 = await tree.search([
        { columnPosition: 0, comparisonType: "EQUAL", value: 2 },
      ]);
      expect(r2).toEqual([rid(0, 1)]);
    });
  });
});
