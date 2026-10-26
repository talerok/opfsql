import { describe, expect, it } from "vitest";
import {
  IoError,
  MisuseError,
  UnsupportedFormatError,
} from "../../../errors.js";
import type { SyncFile } from "../../file/sync-file.js";
import { MemoryFile } from "../../file/sync-file.js";
import type { StorageFiles } from "../../open.js";
import { openPager } from "../../open.js";
import {
  beginRead,
  beginWrite,
  memoryFiles as newFiles,
  read,
  write,
} from "../../tests/pager-support.js";
import { SCHEMA_ROOT_PAGE } from "../database-file.js";
import type { Pager } from "../pager.js";
import type { WriteTransaction } from "../transaction.js";

const PAGE_SIZE = 4096;
/** docs/file-format.md, WAL file: the header is 20 bytes, the epoch at 12. */
const WAL_HEADER_SIZE = 20;
const WAL_EPOCH_OFFSET = 12;
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

function setWalEpoch(wal: SyncFile, epoch: number): void {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, epoch);
  wal.write(bytes, { at: WAL_EPOCH_OFFSET });
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
    const transaction = beginWrite(pager);
    const pageNo = transaction.allocate();
    transaction.rollback();
    expect(pager.latestCommit()).toBe(0);

    const next = beginWrite(pager);
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
    const transaction = beginWrite(await open(newFiles()));
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
  it("sees the other tab's commits after catching up", async () => {
    const { first, second } = await twoTabs();
    let pageNo = 0;
    write(first, (transaction) => {
      pageNo = allocateMarked(transaction, 9);
    });
    expect(second.catchUp()).toBe(true);
    expect(firstByte(second, pageNo)).toBe(9);
    expect(second.catchUp()).toBe(false);
  });

  it("makes a tab that fell behind a checkpoint catch up before reading", async () => {
    const { first, second } = await twoTabs();
    write(first, (transaction) => allocateMarked(transaction, 1));
    write(first, (transaction) => allocateMarked(transaction, 2));
    first.checkpoint();

    expect(second.beginRead()).toBeNull();
    second.catchUp();
    const reader = beginRead(second);
    expect(reader.snapshot).toBe(2);
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
    second.catchUp();
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
    second.catchUp();
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
    expect(fresh.catchUp()).toBe(false);

    expect(fresh.resetStaleIntent()).toBe(true);
    beginRead(fresh).end();
    expect(fresh.resetStaleIntent()).toBe(false);
  });
});
