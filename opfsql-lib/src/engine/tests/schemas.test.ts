import { describe, expect, it } from "vitest";
import { InternalError } from "../../errors.js";
import { MemoryFile } from "../../storage/file/sync-file.js";
import { Schema } from "../../schema/index.js";
import { openPager, type ReadTransaction } from "../../storage/index.js";
import { indexKeysOf } from "../index-keys.js";
import { SchemaCache } from "../schemas.js";

// The schema cache serves committed schemas only: a version a transaction
// has not committed may be rolled back and its number reused.

async function newPager() {
  const files = {
    database: new MemoryFile(),
    wal: new MemoryFile(),
    marks: null,
  };
  return openPager(files, { name: "schemas" });
}

describe("SchemaCache", () => {
  it("loads the schema of a write transaction that has changed nothing yet", async () => {
    const pager = await newPager();
    const transaction = pager.beginWrite();
    const keys = new SchemaCache().load(transaction);
    expect(keys.schema.tables).toEqual([]);
    transaction.rollback();
  });

  it("refuses a snapshot with changes of its own", async () => {
    const pager = await newPager();
    const transaction = pager.beginWrite();
    transaction.bumpSchemaVersion();
    expect(() => new SchemaCache().load(transaction)).toThrow(InternalError);
    transaction.rollback();
  });

  it("keeps one object per committed version", async () => {
    const pager = await newPager();
    const cache = new SchemaCache();
    const first = pager.beginRead();
    const second = pager.beginRead();
    expect(first).not.toBeNull();
    expect(cache.load(first!)).toBe(cache.load(second!));
    first!.end();
    second!.end();
  });

  it("keeps the eight versions used last, a hit making one the most recent", () => {
    const cache = new SchemaCache();
    const versions = Array.from({ length: 8 }, (_, index) => index + 1);
    const keys = versions.map((version) =>
      indexKeysOf(new Schema(version, [], [])),
    );
    for (const each of keys) cache.remember(each);

    // Hits, the newest and an older one, read nothing from the pages.
    expect(cache.load(snapshotAt(8))).toBe(keys[7]);
    expect(cache.load(snapshotAt(1))).toBe(keys[0]);
    expect(cache.load(snapshotAt(1))).toBe(keys[0]);
    // A ninth version evicts the least recently used: 2, not 1.
    cache.remember(indexKeysOf(new Schema(9, [], [])));
    expect(cache.load(snapshotAt(1))).toBe(keys[0]);
    expect(() => cache.load(snapshotAt(2))).toThrow(/no pages/);
  });
});

/**
 * A committed snapshot at schema `version` with no pages: loading from it
 * works only from the cache.
 */
function snapshotAt(version: number): ReadTransaction {
  const snapshot = {
    hasChanges: () => false,
    schemaVersion: version,
    pageSize: 4096,
    read: noPages,
    treeVersion: noPages,
  };
  return snapshot as unknown as ReadTransaction;
}

function noPages(): never {
  throw new Error("no pages: the schema should have come from the cache");
}
