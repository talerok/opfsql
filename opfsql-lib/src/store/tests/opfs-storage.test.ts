import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { OPFSSyncStorage } from "../backend/opfs-storage.js";

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

  it("writePage + readPage roundtrip", async () => {
    const storage = new OPFSSyncStorage("test-db");
    await storage.open();
    const data = { key: "value", nums: [1, 2, 3] };
    storage.writePage(3, data);
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

  it("writePage throws when data exceeds page capacity", async () => {
    const storage = new OPFSSyncStorage("test-db");
    await storage.open();
    // Create an object with a very large string to exceed 32KB page size
    const hugeData = { payload: "x".repeat(40000) };
    expect(() => storage.writePage(3, hugeData)).toThrow(/exceeds page capacity/);
    storage.close();
  });

  it("writeHeader updates nextPageId", async () => {
    const storage = new OPFSSyncStorage("test-db");
    await storage.open();
    expect(storage.getNextPageId()).toBe(3);
    storage.writeHeader(10);
    expect(storage.getNextPageId()).toBe(10);
    storage.close();
  });

  it("persists data across reopen", async () => {
    const storage1 = new OPFSSyncStorage("persist-db");
    await storage1.open();
    storage1.writePage(3, { hello: "world" });
    storage1.writeHeader(4);
    storage1.flush();
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
    storage.writePage(3, { a: 1 });
    storage.writePage(4, { b: 2 });
    storage.writePage(5, { c: 3 });
    storage.writeHeader(6);
    storage.flush();
    // Now shrink to page 4
    storage.writeHeader(4);
    storage.truncateToSize();
    // Pages beyond 4 should be gone
    expect(storage.readPage(5)).toBeNull();
    storage.close();
  });

  it("flush does not throw", async () => {
    const storage = new OPFSSyncStorage("flush-db");
    await storage.open();
    expect(() => storage.flush()).not.toThrow();
    storage.close();
  });

  it("multiple pages roundtrip", async () => {
    const storage = new OPFSSyncStorage("multi-db");
    await storage.open();
    for (let i = 3; i < 10; i++) {
      storage.writePage(i, { page: i, data: `page-${i}` });
    }
    for (let i = 3; i < 10; i++) {
      expect(storage.readPage(i)).toEqual({ page: i, data: `page-${i}` });
    }
    storage.close();
  });

  it("overwrite page replaces data", async () => {
    const storage = new OPFSSyncStorage("overwrite-db");
    await storage.open();
    storage.writePage(3, { version: 1 });
    expect(storage.readPage(3)).toEqual({ version: 1 });
    storage.writePage(3, { version: 2 });
    expect(storage.readPage(3)).toEqual({ version: 2 });
    storage.close();
  });
});

describe("OPFSSyncStorage — open/close lifecycle", () => {
  it("open() is idempotent — second call is no-op", async () => {
    const storage = new OPFSSyncStorage("idem-db");
    await storage.open();
    storage.writePage(3, { val: 1 });
    // Second open should not reset state
    await storage.open();
    expect(storage.readPage(3)).toEqual({ val: 1 });
    storage.close();
  });

  it("close() + open() allows reuse", async () => {
    const storage = new OPFSSyncStorage("reopen-db");
    await storage.open();
    storage.writePage(3, { val: 42 });
    storage.flush();
    storage.close();

    await storage.open();
    expect(storage.readPage(3)).toEqual({ val: 42 });
    storage.close();
  });
});

describe("OPFSSyncStorage — header validation", () => {
  it("rejects file with bad magic", async () => {
    // Create a file with garbage header
    const root = await navigator.storage.getDirectory();
    const fh = await root.getFileHandle("bad-magic.opfsql", { create: true });
    const handle = await fh.createSyncAccessHandle();
    const buf = new Uint8Array(16);
    buf.set(new TextEncoder().encode("GARBAGE!"), 0);
    handle.write(buf, { at: 0 });
    handle.close();

    const storage = new OPFSSyncStorage("bad-magic");
    await expect(storage.open()).rejects.toThrow(/bad magic/);
  });

  it("rejects file with non-power-of-two pageSize", async () => {
    const root = await navigator.storage.getDirectory();
    const fh = await root.getFileHandle("bad-ps.opfsql", { create: true });
    const handle = await fh.createSyncAccessHandle();
    const buf = new Uint8Array(16);
    // Write valid magic
    buf.set(new Uint8Array([0x4f, 0x50, 0x46, 0x53, 0x51, 0x4c, 0x30, 0x33]), 0);
    // Write invalid pageSize (5000 — not a power of 2)
    new DataView(buf.buffer).setUint32(8, 5000, false);
    new DataView(buf.buffer).setUint32(12, 3, false);
    handle.write(buf, { at: 0 });
    handle.close();

    const storage = new OPFSSyncStorage("bad-ps");
    await expect(storage.open()).rejects.toThrow(/bad pageSize/);
  });

  it("rejects file with pageSize too small", async () => {
    const root = await navigator.storage.getDirectory();
    const fh = await root.getFileHandle("small-ps.opfsql", { create: true });
    const handle = await fh.createSyncAccessHandle();
    const buf = new Uint8Array(16);
    buf.set(new Uint8Array([0x4f, 0x50, 0x46, 0x53, 0x51, 0x4c, 0x30, 0x33]), 0);
    // pageSize=1024 < 4096 minimum
    new DataView(buf.buffer).setUint32(8, 1024, false);
    new DataView(buf.buffer).setUint32(12, 3, false);
    handle.write(buf, { at: 0 });
    handle.close();

    const storage = new OPFSSyncStorage("small-ps");
    await expect(storage.open()).rejects.toThrow(/bad pageSize/);
  });
});
