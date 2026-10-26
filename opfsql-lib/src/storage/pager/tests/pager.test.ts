import { describe, expect, it, vi } from "vitest";
import {
  CorruptError,
  InternalError,
  IoError,
  MisuseError,
  StorageFullError,
  UnsupportedFormatError,
} from "../../../errors.js";
import { crc32 } from "../../file/crc32.js";
import type { SyncFile } from "../../file/sync-file.js";
import { MemoryFile } from "../../file/sync-file.js";
import type { StorageFiles } from "../../open.js";
import { openPager } from "../../open.js";
import {
  beginRead,
  memoryFiles as newFiles,
  read,
  write,
} from "../../tests/pager-support.js";
import { SCHEMA_ROOT_PAGE } from "../database-file.js";
import type { Pager } from "../pager.js";
import type { WriteTransaction } from "../transaction.js";

const PAGE_SIZE = 4096;
/** docs/file-format.md, WAL file: the header is 24 bytes, the epoch at 12. */
const WAL_HEADER_SIZE = 24;
const WAL_EPOCH_OFFSET = 12;
const WAL_CHECKSUM_OFFSET = 20;
const TABLE_LEAF_TYPE = 0x02;

/** The same files, as another engine opening them would see them. */
function reopenFiles(files: StorageFiles): StorageFiles {
  return { ...files };
}

async function open(
  files: StorageFiles,
  options: { checkpointThreshold?: number; lockName?: string } = {},
): Promise<Pager> {
  return openPager(files, { name: "test", pageSize: PAGE_SIZE, ...options });
}

/** Allocates a page and marks its first byte with `value`. */
function allocateMarked(transaction: WriteTransaction, value: number): number {
  const pageNo = transaction.allocate();
  transaction.write(pageNo).set([value]);
  return pageNo;
}

function firstByte(pager: Pager, pageNo: number): number {
  return read(pager, (transaction) => transaction.read(pageNo)[0] ?? -1);
}

/** Rewrites the WAL header's epoch, and its checksum to match. */
function setWalEpoch(wal: SyncFile, epoch: number): void {
  const header = new Uint8Array(WAL_HEADER_SIZE);
  wal.read(header, { at: 0 });
  const view = new DataView(header.buffer);
  view.setUint32(WAL_EPOCH_OFFSET, epoch);
  const checksum = crc32(header.subarray(0, WAL_CHECKSUM_OFFSET));
  view.setUint32(WAL_CHECKSUM_OFFSET, checksum);
  wal.write(header, { at: 0 });
}

describe("Pager", () => {
  it("starts an empty database with its header, first map and schema root", async () => {
    const files = newFiles();
    const transaction = beginRead(await open(files));
    expect(files.database.getSize()).toBe(3 * PAGE_SIZE);
    expect(transaction.schemaVersion).toBe(0);
    expect(transaction.read(SCHEMA_ROOT_PAGE)[0]).toBe(TABLE_LEAF_TYPE);
    transaction.end();
  });

  it("recovers committed transactions from the WAL after reopening", async () => {
    const files = newFiles();
    const pager = await open(files);
    let pageNo = 0;
    write(pager, (transaction) => {
      pageNo = allocateMarked(transaction, 42);
      transaction.bumpSchemaVersion();
    });

    const reopened = await open(reopenFiles(files));
    expect(firstByte(reopened, pageNo)).toBe(42);
    expect(read(reopened, (transaction) => transaction.schemaVersion)).toBe(1);
  });

  it("ignores a torn transaction at the end of the WAL", async () => {
    const files = newFiles();
    const pager = await open(files);
    let kept = 0;
    write(pager, (transaction) => {
      kept = allocateMarked(transaction, 1);
    });
    const committedSize = files.wal.getSize();
    write(pager, (transaction) => {
      allocateMarked(transaction, 2);
    });
    files.wal.truncate(committedSize + 100);

    const reopened = await open(reopenFiles(files));
    expect(reopened.latestCommit()).toBe(1);
    expect(firstByte(reopened, kept)).toBe(1);
  });

  it("never replays a frame from another WAL generation", async () => {
    const files = newFiles();
    const pager = await open(files);
    write(pager, (transaction) => allocateMarked(transaction, 1));

    // The next transaction id, a valid checksum, but the header says the WAL
    // has since moved to generation 1: the frames belong to generation 0.
    setWalEpoch(files.wal, 1);
    const reopened = await open(reopenFiles(files));
    expect(reopened.latestCommit()).toBe(0);
  });

  it("reads a page only at a snapshot some transaction has pinned", async () => {
    const pager = await open(newFiles());
    const latest = pager.latestCommit();
    expect(() => pager.read(SCHEMA_ROOT_PAGE, latest)).toThrow(InternalError);
    const transaction = beginRead(pager);
    expect(pager.read(SCHEMA_ROOT_PAGE, latest)[0]).toBe(TABLE_LEAF_TYPE);
    transaction.end();
    expect(() => pager.read(SCHEMA_ROOT_PAGE, latest)).toThrow(InternalError);
  });

  it("keeps a snapshot's pages while later transactions commit", async () => {
    const pager = await open(newFiles());
    let pageNo = 0;
    write(pager, (transaction) => {
      pageNo = allocateMarked(transaction, 1);
    });
    const reader = beginRead(pager);
    write(pager, (transaction) => {
      transaction.write(pageNo).set([2]);
    });

    expect(reader.read(pageNo)[0]).toBe(1);
    expect(firstByte(pager, pageNo)).toBe(2);
    reader.end();
  });

  it("drops a rolled back transaction", async () => {
    const pager = await open(newFiles());
    const transaction = pager.beginWrite();
    const pageNo = transaction.allocate();
    transaction.rollback();
    expect(pager.latestCommit()).toBe(0);

    const next = pager.beginWrite();
    expect(next.allocate()).toBe(pageNo);
    next.rollback();
  });

  it("checkpoints no further than an open snapshot, then the rest", async () => {
    const files = newFiles();
    const pager = await open(files);
    let pageNo = 0;
    write(pager, (transaction) => {
      pageNo = allocateMarked(transaction, 1);
    });
    const reader = beginRead(pager);
    write(pager, (transaction) => {
      transaction.write(pageNo).set([2]);
    });

    pager.checkpoint();
    expect(reader.read(pageNo)[0]).toBe(1);
    expect(files.wal.getSize()).toBeGreaterThan(WAL_HEADER_SIZE);

    reader.end();
    pager.checkpoint();
    expect(files.wal.getSize()).toBe(WAL_HEADER_SIZE);
    expect(firstByte(await open(reopenFiles(files)), pageNo)).toBe(2);
  });

  it("allocates the lowest free page and truncates a free tail on checkpoint", async () => {
    const files = newFiles();
    const pager = await open(files);
    const pages: number[] = [];
    write(pager, (transaction) => {
      for (let count = 0; count < 20; count++) {
        pages.push(transaction.allocate());
      }
    });
    expect(pages).toEqual(Array.from({ length: 20 }, (_, index) => index + 3));

    const kept = pages.slice(0, 10);
    write(pager, (transaction) => {
      transaction.free(7);
      for (const pageNo of pages.slice(10)) transaction.free(pageNo);
    });
    write(pager, (transaction) => {
      expect(transaction.allocate()).toBe(7);
    });
    pager.checkpoint();
    const lastKept = kept.at(-1) ?? 0;
    expect(files.database.getSize()).toBe((lastKept + 1) * PAGE_SIZE);
  });

  it("refuses commits after a failed append it could not undo", async () => {
    const files = newFiles();
    const pager = await open(files);
    files.wal.write = () => {
      throw new Error("disk full");
    };
    files.wal.truncate = () => {
      throw new Error("still failing");
    };

    expect(() => {
      write(pager, (transaction) => allocateMarked(transaction, 1));
    }).toThrow(IoError);
    expect(() => {
      write(pager, (transaction) => allocateMarked(transaction, 1));
    }).toThrow(/unknown state/);
  });

  it("fails every later append with an error of its own, caused by the first", async () => {
    const files = newFiles();
    const pager = await open(files);
    files.wal.write = () => {
      throw new Error("disk full");
    };
    files.wal.truncate = () => {
      throw new Error("still failing");
    };
    const failureOf = (): unknown => {
      try {
        write(pager, (transaction) => allocateMarked(transaction, 1));
      } catch (error) {
        return error;
      }
      return null;
    };

    failureOf();
    const second = failureOf();
    const third = failureOf();

    expect(second).toBeInstanceOf(IoError);
    expect(third).toBeInstanceOf(IoError);
    expect(third).not.toBe(second);
    expect((third as IoError).cause).toBe((second as IoError).cause);
  });

  it("calls a page freed twice damage: the map and the trees disagree", async () => {
    const pager = await open(newFiles());
    const transaction = pager.beginWrite();
    const pageNo = transaction.allocate();
    transaction.free(pageNo);
    expect(() => {
      transaction.free(pageNo);
    }).toThrow(CorruptError);
    transaction.rollback();
  });

  it("keeps a committed page apart from the buffer it was written in", async () => {
    const pager = await open(newFiles());
    const transaction = pager.beginWrite();
    const pageNo = transaction.allocate();
    const page = transaction.write(pageNo);
    page.set([1]);
    transaction.commit();

    // A buffer kept past the commit changes nothing committed.
    page.set([99]);

    expect(firstByte(pager, pageNo)).toBe(1);
  });

  it("reports a write the browser refuses for want of space as STORAGE_FULL", async () => {
    const files = newFiles();
    const pager = await open(files);
    const writeFile = files.wal.write.bind(files.wal);
    let shouldFail = true;
    files.wal.write = (buffer, options) => {
      if (!shouldFail) return writeFile(buffer, options);
      shouldFail = false;
      throw new DOMException("quota", "QuotaExceededError");
    };

    expect(() => {
      write(pager, (transaction) => allocateMarked(transaction, 1));
    }).toThrow(StorageFullError);
    expect(pager.latestCommit()).toBe(0);
    write(pager, (transaction) => allocateMarked(transaction, 1));
    expect(pager.latestCommit()).toBe(1);
  });

  it("rolls back an append whose flush fails", async () => {
    const files = newFiles();
    const pager = await open(files);
    const flush = files.wal.flush.bind(files.wal);
    let shouldFail = true;
    files.wal.flush = () => {
      if (shouldFail) {
        shouldFail = false;
        throw new Error("flush failed");
      }
      flush();
    };

    expect(() => {
      write(pager, (transaction) => allocateMarked(transaction, 1));
    }).toThrow(IoError);
    expect(pager.latestCommit()).toBe(0);
    write(pager, (transaction) => allocateMarked(transaction, 1));
    expect(pager.latestCommit()).toBe(1);
  });

  it("refuses any use of a transaction after it ends", async () => {
    const pager = await open(newFiles());
    const transaction = pager.beginWrite();
    const pageNo = allocateMarked(transaction, 1);
    transaction.commit();
    expect(() => transaction.write(pageNo)).toThrow(MisuseError);
    expect(() => transaction.read(pageNo)).toThrow(MisuseError);
    expect(() => {
      transaction.commit();
    }).toThrow(MisuseError);
  });

  it("refuses files of another format", async () => {
    const files = newFiles();
    files.database.write(new TextEncoder().encode("OPFSQL03"), { at: 0 });
    await expect(open(files)).rejects.toThrow(UnsupportedFormatError);
  });
});

/** docs/file-format.md, Read-mark file: the intent at 16, slot marks from 24. */
const INTENT_OFFSET = 16;
const MARKS_OFFSET = 24;
/** A slot no tab in these tests claims. */
const ORPHAN_SLOT = 63;

function intentIn(marks: MemoryFile): number {
  const bytes = new Uint8Array(8);
  marks.read(bytes, { at: INTENT_OFFSET });
  return new DataView(bytes.buffer).getFloat64(0);
}

function setMark(marks: MemoryFile, slot: number, snapshot: number): void {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, snapshot);
  marks.write(bytes, { at: MARKS_OFFSET + slot * 8 });
}

let tabsOpened = 0;

/** Two tabs sharing one database's files. */
async function twoTabs(): Promise<{
  first: Pager;
  second: Pager;
  marks: MemoryFile;
}> {
  const database = new MemoryFile();
  const wal = new MemoryFile();
  const marks = new MemoryFile();
  const lockName = `tabs-${String(tabsOpened++)}`;
  const first = await open({ database, wal, marks }, { lockName });
  const second = await open({ database, wal, marks }, { lockName });
  return { first, second, marks };
}

describe("Pager across tabs", () => {
  it("begins every snapshot at the latest commit of any tab", async () => {
    const { first, second } = await twoTabs();
    let pageNo = 0;
    write(first, (transaction) => {
      pageNo = allocateMarked(transaction, 9);
    });
    expect(firstByte(second, pageNo)).toBe(9);
  });

  it("reads past another tab's checkpoint at the commit it reached", async () => {
    const { first, second } = await twoTabs();
    write(first, (transaction) => allocateMarked(transaction, 1));
    write(first, (transaction) => allocateMarked(transaction, 2));
    first.checkpoint();

    const reader = beginRead(second);
    expect(reader.snapshot).toBe(2);
    reader.end();
  });

  // A tab that last read before another tab's commit decides nothing on that
  // stale view: each operation under the write lock reads the commit first.

  it("never drops a commit the checkpointing tab had not read", async () => {
    const { first, second } = await twoTabs();
    let pageNo = 0;
    write(first, (transaction) => {
      pageNo = allocateMarked(transaction, 1);
    });
    expect(firstByte(second, pageNo)).toBe(1);
    write(first, (transaction) => {
      transaction.write(pageNo).set([2]);
    });

    second.checkpoint();

    expect(firstByte(first, pageNo)).toBe(2);
    expect(firstByte(second, pageNo)).toBe(2);
  });

  it("writes on top of a commit the writing tab had not read", async () => {
    const { first, second } = await twoTabs();
    let pageNo = 0;
    write(first, (transaction) => {
      pageNo = allocateMarked(transaction, 1);
    });
    expect(firstByte(second, pageNo)).toBe(1);
    write(first, (transaction) => {
      transaction.write(pageNo).set([2]);
    });

    write(second, (transaction) => {
      expect(transaction.read(pageNo)[0]).toBe(2);
      transaction.write(pageNo).set([3]);
    });

    expect(firstByte(first, pageNo)).toBe(3);
  });

  it("keeps a checkpoint intent that commits the tab had not read back", async () => {
    const { first, second } = await twoTabs();
    expect(second.latestCommit()).toBe(0);
    write(first, (transaction) => allocateMarked(transaction, 1));
    write(first, (transaction) => allocateMarked(transaction, 2));
    first.checkpoint();

    expect(second.resetStaleIntent()).toBe(false);
    expect(beginRead(second).snapshot).toBe(2);
  });

  it("refuses to commit over another tab's commit, as without the write lock", async () => {
    const { first, second } = await twoTabs();
    let pageNo = 0;
    write(first, (transaction) => {
      pageNo = allocateMarked(transaction, 1);
    });
    const writing = second.beginWrite();
    write(first, (transaction) => {
      transaction.write(pageNo).set([2]);
    });
    writing.write(pageNo).set([3]);

    expect(() => {
      writing.commit();
    }).toThrow(InternalError);
    expect(firstByte(first, pageNo)).toBe(2);
    expect(firstByte(second, pageNo)).toBe(2);
  });

  it("never lowers the intent, even for a mark below what the file holds", async () => {
    const { first, marks } = await twoTabs();
    write(first, (transaction) => allocateMarked(transaction, 1));
    write(first, (transaction) => allocateMarked(transaction, 2));
    first.checkpoint();
    expect(intentIn(marks)).toBe(2);

    // A tab publishes a mark at 0, about to read the intent and be refused.
    setMark(marks, ORPHAN_SLOT, 0);
    write(first, (transaction) => allocateMarked(transaction, 3));
    first.checkpoint();

    expect(intentIn(marks)).toBe(3);
  });

  it("keeps a snapshot's page count when another tab truncates the free tail", async () => {
    const { first, second } = await twoTabs();
    let tail = 0;
    write(first, (transaction) => {
      allocateMarked(transaction, 1);
      tail = allocateMarked(transaction, 2);
    });
    write(first, (transaction) => {
      transaction.free(tail);
    });
    const reader = beginRead(second);
    const pageCount = reader.pageCount;

    first.checkpoint();
    // Another transaction of the reader's tab reads the restarted WAL.
    beginRead(second).end();

    expect(reader.pageCount).toBe(pageCount);
    reader.end();
  });

  it("never checkpoints past a snapshot the other tab holds", async () => {
    const { first, second } = await twoTabs();
    let pageNo = 0;
    write(first, (transaction) => {
      pageNo = allocateMarked(transaction, 1);
    });
    first.checkpoint();

    // The second tab reads the page from the database file, at snapshot 1.
    const reader = beginRead(second);
    expect(reader.read(pageNo)[0]).toBe(1);

    // The first tab changes it and checkpoints, unaware of the snapshot but
    // for the read marks.
    write(first, (transaction) => {
      transaction.write(pageNo).set([2]);
    });
    first.checkpoint();
    expect(reader.read(pageNo)[0]).toBe(1);

    reader.end();
    first.checkpoint();
    expect(firstByte(second, pageNo)).toBe(2);
  });

  it("lowers a checkpoint intent left by an earlier database of the same name", async () => {
    const { first, marks } = await twoTabs();
    write(first, (transaction) => allocateMarked(transaction, 1));
    first.checkpoint();

    // The database and WAL files are removed; the read marks stay.
    const lockName = `tabs-${String(tabsOpened++)}`;
    const fresh = await open(
      { database: new MemoryFile(), wal: new MemoryFile(), marks },
      { lockName },
    );
    expect(fresh.beginRead()).toBeNull();

    expect(fresh.resetStaleIntent()).toBe(true);
    beginRead(fresh).end();
    expect(fresh.resetStaleIntent()).toBe(false);
  });

  it("fails to open, rather than wait for good, when the browser refuses a slot's lock", async () => {
    const refusal = new DOMException("refused", "SecurityError");
    const locks = navigator.locks;
    const request = locks.request.bind(locks);
    const refuseSlots = vi
      .spyOn(locks, "request")
      .mockImplementation(async (name: string, ...rest: unknown[]) =>
        name.includes(":slot:")
          ? Promise.reject(refusal)
          : (request as (...all: unknown[]) => Promise<unknown>)(name, ...rest),
      );
    try {
      const lockName = `tabs-${String(tabsOpened++)}`;
      const files = {
        database: new MemoryFile(),
        wal: new MemoryFile(),
        marks: new MemoryFile(),
      };
      await expect(open(files, { lockName })).rejects.toBe(refusal);
    } finally {
      refuseSlots.mockRestore();
    }
  });
});
