import { beforeEach, describe, expect, it } from "vitest";
import type { IndexKey } from "../index-btree/types.js";
import { IndexManager } from "../index-manager.js";
import { PageManager } from "../page-manager.js";
import type { RowId } from "../types.js";
import { MemoryStorage } from "./memory-storage.js";

function rid(a: number, b: number): RowId {
  return a * 1000 + b;
}

describe("IndexManager", () => {
  let pm: PageManager;
  let im: IndexManager;

  beforeEach(() => {
    pm = new PageManager(new MemoryStorage());
    im = new IndexManager(pm);
  });

  // -------------------------------------------------------------------------
  // Basic CRUD through IndexManager
  // -------------------------------------------------------------------------

  describe("insert and search", () => {
    it("insert and find a single entry", async () => {
      await im.insert("idx1", [10], rid(0, 0), false);
      await pm.commit();

      const results = await im.search("idx1", [
        { columnPosition: 0, comparisonType: "EQUAL", value: 10 },
      ]);
      expect(results).toEqual([rid(0, 0)]);
    });

    it("insert with unique=true enforces uniqueness", async () => {
      await im.insert("idx1", [1], rid(0, 0), true);

      await expect(im.insert("idx1", [1], rid(0, 1), true)).rejects.toThrow(
        "UNIQUE constraint failed",
      );
    });

    it("different indexes are independent", async () => {
      await im.insert("idx_a", [1], rid(0, 0), false);
      await im.insert("idx_b", [1], rid(0, 1), false);
      await pm.commit();

      const ra = await im.search("idx_a", [
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(ra).toEqual([rid(0, 0)]);

      const rb = await im.search("idx_b", [
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(rb).toEqual([rid(0, 1)]);
    });
  });

  describe("delete", () => {
    it("delete removes entry from index", async () => {
      await im.insert("idx1", [5], rid(0, 0), false);
      await pm.commit();

      await im.delete("idx1", [5], rid(0, 0));
      await pm.commit();

      const results = await im.search("idx1", [
        { columnPosition: 0, comparisonType: "EQUAL", value: 5 },
      ]);
      expect(results).toEqual([]);
    });
  });

  // -------------------------------------------------------------------------
  // Bulk load
  // -------------------------------------------------------------------------

  describe("bulkLoad", () => {
    it("bulk load creates searchable index", async () => {
      const entries = Array.from({ length: 20 }, (_, i) => ({
        key: [i * 10] as IndexKey,
        rowId: rid(0, i),
      }));
      await im.bulkLoad("idx1", entries, false);
      await pm.commit();

      const results = await im.search("idx1", [
        { columnPosition: 0, comparisonType: "EQUAL", value: 50 },
      ]);
      expect(results).toEqual([rid(0, 5)]);
    });

    it("bulk load with unique=true rejects duplicates", async () => {
      const entries = [
        { key: [1] as IndexKey, rowId: rid(0, 0) },
        { key: [1] as IndexKey, rowId: rid(0, 1) },
      ];
      await expect(im.bulkLoad("idx1", entries, true)).rejects.toThrow(
        "UNIQUE constraint failed",
      );
    });
  });

  // -------------------------------------------------------------------------
  // Drop index
  // -------------------------------------------------------------------------

  describe("dropIndex", () => {
    it("drop removes all index data", async () => {
      await im.insert("idx1", [1], rid(0, 0), false);
      await im.insert("idx1", [2], rid(0, 1), false);
      await pm.commit();

      await im.dropIndex("idx1");
      await pm.commit();

      const results = await im.search("idx1", [
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(results).toEqual([]);
    });

    it("drop one index does not affect another", async () => {
      await im.insert("idx_a", [1], rid(0, 0), false);
      await im.insert("idx_b", [1], rid(0, 1), false);
      await pm.commit();

      await im.dropIndex("idx_a");
      await pm.commit();

      const ra = await im.search("idx_a", [
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(ra).toEqual([]);

      const rb = await im.search("idx_b", [
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(rb).toEqual([rid(0, 1)]);
    });
  });

  // -------------------------------------------------------------------------
  // Search with totalColumns (prefix scan vs point lookup)
  // -------------------------------------------------------------------------

  describe("search with totalColumns", () => {
    it("prefix scan with totalColumns", async () => {
      await im.insert("idx1", ["a", 1], rid(0, 0), false);
      await im.insert("idx1", ["a", 2], rid(0, 1), false);
      await im.insert("idx1", ["b", 1], rid(0, 2), false);
      await pm.commit();

      const results = await im.search(
        "idx1",
        [{ columnPosition: 0, comparisonType: "EQUAL", value: "a" }],
        2,
      );
      expect(results).toHaveLength(2);
      expect(results).toContainEqual(rid(0, 0));
      expect(results).toContainEqual(rid(0, 1));
    });

    it("point lookup with all columns covered", async () => {
      await im.insert("idx1", ["a", 1], rid(0, 0), false);
      await im.insert("idx1", ["a", 2], rid(0, 1), false);
      await pm.commit();

      const results = await im.search(
        "idx1",
        [
          { columnPosition: 0, comparisonType: "EQUAL", value: "a" },
          { columnPosition: 1, comparisonType: "EQUAL", value: 1 },
        ],
        2,
      );
      expect(results).toEqual([rid(0, 0)]);
    });
  });

  // -------------------------------------------------------------------------
  // Index name case insensitivity
  // -------------------------------------------------------------------------

  describe("case insensitivity", () => {
    it("index names are lowercased", async () => {
      await im.insert("MyIndex", [1], rid(0, 0), false);
      await pm.commit();

      const results = await im.search("myindex", [
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
      ]);
      expect(results).toEqual([rid(0, 0)]);
    });
  });
});
