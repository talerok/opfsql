import { describe, expect, it } from "vitest";
import { CorruptError } from "../../../errors.js";
import type { SyncFile } from "../../file/sync-file.js";
import { MemoryFile } from "../../file/sync-file.js";
import { openPager } from "../../open.js";
import { read, write } from "../../tests/pager-support.js";
import type { Pager } from "../pager.js";

// A WAL restart as another tab sees it (docs/file-format.md, WAL file): the
// tab may catch up between any two of the restarting tab's file operations,
// and may read the header while it is being rewritten.

const PAGE_SIZE = 4096;
/** docs/file-format.md, WAL file: the header is 24 bytes. */
const WAL_HEADER_SIZE = 24;

let tabsOpened = 0;

/** `file`, running `step` before and after each write and truncate. */
function interleaved(file: SyncFile, step: () => void): SyncFile {
  const around = <T>(change: () => T): T => {
    step();
    const result = change();
    step();
    return result;
  };
  return {
    read: (buffer, options) => file.read(buffer, options),
    write: (buffer, options) => around(() => file.write(buffer, options)),
    getSize: () => file.getSize(),
    truncate: (size) => {
      around(() => {
        file.truncate(size);
      });
    },
    flush: () => {
      file.flush();
    },
    close: () => {
      file.close();
    },
  };
}

/** `file`, whose next `count` header reads come back torn: one byte flipped. */
function tornHeader(file: SyncFile): {
  file: SyncFile;
  tear: (count: number) => void;
  left: () => number;
} {
  let tornLeft = 0;
  const torn: SyncFile = {
    read: (buffer, options) => {
      const bytesRead = file.read(buffer, options);
      const isHeader = options.at === 0 && buffer.length === WAL_HEADER_SIZE;
      if (isHeader && tornLeft > 0) {
        tornLeft--;
        buffer[16] = (buffer[16] ?? 0) ^ 0xff;
      }
      return bytesRead;
    },
    write: (buffer, options) => file.write(buffer, options),
    getSize: () => file.getSize(),
    truncate: (size) => {
      file.truncate(size);
    },
    flush: () => {
      file.flush();
    },
    close: () => {
      file.close();
    },
  };
  return {
    file: torn,
    tear: (count) => {
      tornLeft = count;
    },
    left: () => tornLeft,
  };
}

async function openTab(
  files: { database: SyncFile; wal: SyncFile; marks: SyncFile },
  lockName: string,
): Promise<Pager> {
  return openPager(files, { name: "restart", pageSize: PAGE_SIZE, lockName });
}

/** Commits `value` as the first byte of `pageNo`, allocating it if 0. */
function commitMark(pager: Pager, pageNo: number, value: number): number {
  return write(pager, (transaction) => {
    const page = pageNo === 0 ? transaction.allocate() : pageNo;
    transaction.write(page).set([value]);
    return page;
  });
}

function firstByte(pager: Pager, pageNo: number): number {
  return read(pager, (transaction) => transaction.read(pageNo)[0] ?? -1);
}

describe("a WAL restart seen from another tab", () => {
  it("leaves a whole header and the committed data at every step", async () => {
    const database = new MemoryFile();
    const wal = new MemoryFile();
    const marks = new MemoryFile();
    const lockName = `restart-${String(tabsOpened++)}`;
    const seen: number[] = [];
    let other: Pager | null = null;
    let pageNo = 0;
    const catchUpAndRead = (): void => {
      if (other === null || pageNo === 0) return;
      other.catchUp();
      seen.push(firstByte(other, pageNo));
    };
    const restarting = await openTab(
      { database, wal: interleaved(wal, catchUpAndRead), marks },
      lockName,
    );
    other = await openTab({ database, wal, marks }, lockName);
    pageNo = commitMark(restarting, pageNo, 1);
    commitMark(restarting, pageNo, 2);
    seen.length = 0;

    restarting.checkpoint();

    expect(seen.length).toBeGreaterThan(0);
    expect(new Set(seen)).toEqual(new Set([2]));
    expect(wal.getSize()).toBe(WAL_HEADER_SIZE);
  });

  it("reads a header again when it is caught half rewritten", async () => {
    const database = new MemoryFile();
    const wal = new MemoryFile();
    const marks = new MemoryFile();
    const lockName = `restart-${String(tabsOpened++)}`;
    const writer = await openTab({ database, wal, marks }, lockName);
    const readerWal = tornHeader(wal);
    const reader = await openTab(
      { database, wal: readerWal.file, marks },
      lockName,
    );
    const pageNo = commitMark(writer, 0, 7);
    writer.checkpoint();

    readerWal.tear(3);
    reader.catchUp();

    expect(readerWal.left()).toBe(0);
    expect(reader.latestCommit()).toBe(writer.latestCommit());
    expect(firstByte(reader, pageNo)).toBe(7);
  });

  it("calls a header that keeps failing its checksum corrupt", async () => {
    const database = new MemoryFile();
    const wal = new MemoryFile();
    const marks = new MemoryFile();
    const lockName = `restart-${String(tabsOpened++)}`;
    await openTab({ database, wal, marks }, lockName);
    const readerWal = tornHeader(wal);
    const reader = await openTab(
      { database, wal: readerWal.file, marks },
      lockName,
    );

    readerWal.tear(Infinity);

    expect(() => reader.catchUp()).toThrow(CorruptError);
  });
});
