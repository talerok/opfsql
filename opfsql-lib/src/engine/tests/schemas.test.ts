import { describe, expect, it } from "vitest";
import { InternalError } from "../../errors.js";
import { MemoryFile } from "../../storage/file/sync-file.js";
import { openPager } from "../../storage/index.js";
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
});
