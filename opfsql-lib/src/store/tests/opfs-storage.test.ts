import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { OPFSSyncStorage } from "../backend/opfs-storage.js";
import { PageOverflowError, UnsupportedFormatError } from "../errors.js";
import { writeFormatPrefix } from "../file-format.js";
import { commitBatch } from "./helpers.js";

beforeEach(() => {
  resetMockOPFS();
});

describe("OPFSSyncStorage", () => {
  it("open initializes new file with header and nextPageId=3", async () => {
    const storage = new OPFSSyncStorage("test-db");
    await storage.open();
    expect(storage.getNextPageId()).toBe(3);
    storage.close();
  });

  it("commit + readPage roundtrip", async () => {
    const storage = new OPFSSyncStorage("test-db");
    await storage.open();
    const data = { key: "value", nums: [1, 2, 3] };
    commitBatch(storage, { 3: data });
    const result = storage.readPage(3);
    expect(result).toEqual(data);
    storage.close();
  });

  it("readPage returns null for unwritten page", async () => {
    const storage = new OPFSSyncStorage("test-db");
    await storage.open();
    expect(storage.readPage(99)).toBeNull();
    storage.close();
  });

  it("readPage returns null when payload length is 0", async () => {
    const storage = new OPFSSyncStorage("test-db");
    await storage.open();
    // Page 1 (catalog) is reserved but unwritten — its 4-byte length prefix is 0
    expect(storage.readPage(1)).toBeNull();
    storage.close();
  });

  it("commit throws when a page exceeds page capacity", async () => {
    const storage = new OPFSSyncStorage("test-db");
    await storage.open();
    // Create an object with a very large string to exceed 32KB page size
    const hugeData = { payload: "x".repeat(40000) };
    expect(() => commitBatch(storage, { 3: hugeData })).toThrow(
      PageOverflowError,
    );
    storage.close();
  });

  it("an oversized page rejects the whole batch before anything is written", async () => {
    const storage = new OPFSSyncStorage("test-db");
    await storage.open();
    expect(() =>
      commitBatch(storage, { 3: "small", 4: "x".repeat(40000) }, 5),
    ).toThrow(PageOverflowError);
    expect(storage.readPage(3)).toBeNull();
    expect(storage.getNextPageId()).toBe(3);
    storage.close();
  });

  it("commit updates nextPageId", async () => {
    const storage = new OPFSSyncStorage("test-db");
    await storage.open();
    expect(storage.getNextPageId()).toBe(3);
    commitBatch(storage, {}, 10);
    expect(storage.getNextPageId()).toBe(10);
    storage.close();
  });

  it("persists data across reopen", async () => {
    const storage1 = new OPFSSyncStorage("persist-db");
    await storage1.open();
    commitBatch(storage1, { 3: { hello: "world" } }, 4);
    storage1.close();

    const storage2 = new OPFSSyncStorage("persist-db");
    await storage2.open();
    expect(storage2.getNextPageId()).toBe(4);
    expect(storage2.readPage(3)).toEqual({ hello: "world" });
    storage2.close();
  });

  it("truncateToSize shrinks file", async () => {
    const storage = new OPFSSyncStorage("truncate-db");
    await storage.open();
    // Write several pages
    commitBatch(storage, { 3: { a: 1 }, 4: { b: 2 }, 5: { c: 3 } }, 6);
    // Now shrink to page 4
    commitBatch(storage, {}, 4);
    storage.truncateToSize();
    // Pages beyond 4 should be gone
    expect(storage.readPage(5)).toBeNull();
    storage.close();
  });

  it("multiple pages roundtrip", async () => {
    const storage = new OPFSSyncStorage("multi-db");
    await storage.open();
    const pages: Record<number, unknown> = {};
    for (let i = 3; i < 10; i++) {
      pages[i] = { page: i, data: `page-${i}` };
    }
    commitBatch(storage, pages);
    for (let i = 3; i < 10; i++) {
      expect(storage.readPage(i)).toEqual({ page: i, data: `page-${i}` });
    }
    storage.close();
  });

  it("overwrite page replaces data", async () => {
    const storage = new OPFSSyncStorage("overwrite-db");
    await storage.open();
    commitBatch(storage, { 3: { version: 1 } });
    expect(storage.readPage(3)).toEqual({ version: 1 });
    commitBatch(storage, { 3: { version: 2 } });
    expect(storage.readPage(3)).toEqual({ version: 2 });
    storage.close();
  });
});

describe("OPFSSyncStorage — page size", () => {
  it("defaults to 32 KB pages", async () => {
    const storage = new OPFSSyncStorage("default-ps");
    await storage.open();
    expect(storage.pageCapacity).toBe(32768 - 4);
    storage.close();
  });

  it("pageSize option sets the capacity of a new file", async () => {
    const storage = new OPFSSyncStorage("small-pages", { pageSize: 4096 });
    await storage.open();
    expect(storage.pageCapacity).toBe(4096 - 4);
    expect(() => commitBatch(storage, { 3: "x".repeat(5000) })).toThrow(
      PageOverflowError,
    );
    storage.close();
  });

  it("an existing file keeps the page size from its header", async () => {
    const created = new OPFSSyncStorage("kept-ps", { pageSize: 4096 });
    await created.open();
    created.close();

    const reopened = new OPFSSyncStorage("kept-ps");
    await reopened.open();
    expect(reopened.pageCapacity).toBe(4096 - 4);
    reopened.close();
  });

  it("rejects an invalid pageSize option", () => {
    expect(() => new OPFSSyncStorage("bad", { pageSize: 5000 })).toThrow(
      /Invalid pageSize/,
    );
    expect(() => new OPFSSyncStorage("bad", { pageSize: 2048 })).toThrow(
      /Invalid pageSize/,
    );
  });
});

describe("OPFSSyncStorage — open/close lifecycle", () => {
  it("open() is idempotent — second call is no-op", async () => {
    const storage = new OPFSSyncStorage("idem-db");
    await storage.open();
    commitBatch(storage, { 3: { val: 1 } });
    // Second open should not reset state
    await storage.open();
    expect(storage.readPage(3)).toEqual({ val: 1 });
    storage.close();
  });

  it("close() + open() allows reuse", async () => {
    const storage = new OPFSSyncStorage("reopen-db");
    await storage.open();
    commitBatch(storage, { 3: { val: 42 } });
    storage.close();

    await storage.open();
    expect(storage.readPage(3)).toEqual({ val: 42 });
    storage.close();
  });
});

describe("OPFSSyncStorage — header validation", () => {
  /** Writes a database file header by hand, as another writer would have. */
  async function writeHeader(
    dbName: string,
    fill: (header: Uint8Array, view: DataView) => void,
  ): Promise<void> {
    const root = await navigator.storage.getDirectory();
    const fh = await root.getFileHandle(`${dbName}.opfsql`, { create: true });
    const handle = await fh.createSyncAccessHandle();
    const header = new Uint8Array(20);
    fill(header, new DataView(header.buffer));
    handle.write(header, { at: 0 });
    handle.close();
  }

  const validHeader = (pageSize: number) => (h: Uint8Array, v: DataView) => {
    writeFormatPrefix(h, "database");
    v.setUint32(12, pageSize, false);
    v.setUint32(16, 3, false);
  };

  it("rejects a file that is not an opfsql database", async () => {
    await writeHeader("garbage", (h) =>
      h.set(new TextEncoder().encode("GARBAGE!")),
    );
    await expect(new OPFSSyncStorage("garbage").open()).rejects.toThrow(
      /"garbage.opfsql" is not an opfsql database file/,
    );
  });

  it("rejects a database written in an earlier format", async () => {
    await writeHeader("legacy", (h) =>
      h.set(new TextEncoder().encode("OPFSQL03")),
    );
    const open = new OPFSSyncStorage("legacy").open();
    await expect(open).rejects.toThrow(UnsupportedFormatError);
    await expect(open).rejects.toThrow(
      /uses file format 3, but this opfsql reads format 4/,
    );
  });

  it("rejects a database written in a later format", async () => {
    await writeHeader("future", (h, v) => {
      validHeader(32768)(h, v);
      v.setUint32(8, 5, false);
    });
    await expect(new OPFSSyncStorage("future").open()).rejects.toThrow(
      UnsupportedFormatError,
    );
  });

  it("rejects a page size that is not a power of two", async () => {
    await writeHeader("bad-ps", validHeader(5000));
    await expect(new OPFSSyncStorage("bad-ps").open()).rejects.toThrow(
      /invalid page size 5000/,
    );
  });

  it("rejects a page size below the minimum", async () => {
    await writeHeader("small-ps", validHeader(1024));
    await expect(new OPFSSyncStorage("small-ps").open()).rejects.toThrow(
      /invalid page size 1024/,
    );
  });
});
