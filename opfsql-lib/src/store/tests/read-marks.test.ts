import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { OPFSSyncStorage } from "../backend/opfs-storage.js";
import { StorageError } from "../errors.js";
import { Storage } from "../storage.js";
import { MemoryFileHandle } from "../wal/file-handle.js";
import { ReadMarks } from "../wal/read-marks.js";
import { WalStorage } from "../wal/wal-storage.js";
import { writeOrphanMark } from "./helpers.js";

let seq = 0;
let lockName: string;

beforeEach(() => {
  resetMockOPFS();
  lockName = `opfsql:marks-${seq++}`;
});

describe("ReadMarks", () => {
  it("tabs see each other's oldest snapshots", async () => {
    const shm = new MemoryFileHandle();
    const a = await ReadMarks.open(shm, lockName);
    const b = await ReadMarks.open(shm, lockName);
    expect(a.oldestMark()).toBeNull();

    a.publish(5);
    b.publish(3);
    expect(a.oldestMark()).toBe(3);
    b.publish(null);
    expect(a.oldestMark()).toBe(5);
  });

  it("closing a tab clears its mark", async () => {
    const shm = new MemoryFileHandle();
    const a = await ReadMarks.open(shm, lockName);
    const b = await ReadMarks.open(shm, lockName);
    a.publish(4);
    a.close();
    expect(b.oldestMark()).toBeNull();
  });

  it("shares the checkpoint intent", async () => {
    const shm = new MemoryFileHandle();
    const a = await ReadMarks.open(shm, lockName);
    const b = await ReadMarks.open(shm, lockName);
    a.setCheckpointIntent(7);
    expect(b.checkpointIntent()).toBe(7);
  });

  it("reclaims the mark of a tab that died, never a live tab's", async () => {
    const shm = new MemoryFileHandle();
    const a = await ReadMarks.open(shm, lockName);
    const b = await ReadMarks.open(shm, lockName);
    a.publish(9);
    writeOrphanMark(shm, 40, 2);
    expect(b.oldestMark()).toBe(2);

    await b.reclaimDeadSlots();
    expect(b.oldestMark()).toBe(9);
  });

  it("clears marks left by a crashed session when the database is opened", async () => {
    const shm = new MemoryFileHandle();
    (await ReadMarks.open(shm, lockName)).close();
    writeOrphanMark(shm, 3, 11);
    writeOrphanMark(shm, 7, 12);

    const marks = await ReadMarks.open(shm, lockName);
    expect(marks.oldestMark()).toBeNull();
  });

  it("refuses a tab once every slot is taken", async () => {
    const shm = new MemoryFileHandle();
    for (let i = 0; i < 64; i++) await ReadMarks.open(shm, lockName);
    await expect(ReadMarks.open(shm, lockName)).rejects.toThrow(StorageError);
  });
});

describe("snapshots across tabs", () => {
  async function openTab(
    main: OPFSSyncStorage,
    wal: MemoryFileHandle,
    shm: MemoryFileHandle,
  ) {
    const marks = await ReadMarks.open(shm, lockName);
    const storage = new Storage(new WalStorage(main, wal, { marks }));
    await storage.open();
    return storage;
  }

  it("a tab that has not caught up cannot pin a snapshot behind another tab's checkpoint", async () => {
    const main = new OPFSSyncStorage(`marks-db-${seq}`);
    const wal = new MemoryFileHandle();
    const shm = new MemoryFileHandle();
    const a = await openTab(main, wal, shm);
    const b = await openTab(main, wal, shm);

    const writer = a.createSession();
    const page = writer.allocPage();
    writer.writePage(page, "v1");
    writer.commit();
    b.catchUp(); // b knows transaction 1

    writer.writePage(page, "v2");
    writer.commit();
    a.checkpoint(); // main file now holds transaction 2

    expect(b.tryPinSnapshot()).toBeNull();
    b.catchUp();
    const snapshot = b.tryPinSnapshot();
    expect(snapshot).toBe(2);
    expect(b.readPage(page, snapshot!)).toBe("v2");
  });

  it("a checkpoint stops at the oldest snapshot open in another tab", async () => {
    const main = new OPFSSyncStorage(`marks-db-${seq}`);
    const wal = new MemoryFileHandle();
    const shm = new MemoryFileHandle();
    const a = await openTab(main, wal, shm);
    const b = await openTab(main, wal, shm);

    const writer = a.createSession();
    const page = writer.allocPage();
    writer.writePage(page, "v1");
    writer.commit();
    b.catchUp();
    const snapshot = b.tryPinSnapshot()!;

    writer.writePage(page, "v2");
    writer.commit();
    a.checkpoint();
    // The main file must not move past b's snapshot. It may lag behind it:
    // a keeps no version only b needs, and b reads v1 from its own WAL index.
    expect(main.readPage(page)).not.toBe("v2");
    expect(b.readPage(page, snapshot)).toBe("v1");

    b.releaseSnapshot(snapshot);
    a.checkpoint();
    expect(main.readPage(page)).toBe("v2");
  });
});
