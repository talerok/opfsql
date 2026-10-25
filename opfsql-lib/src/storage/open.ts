import { DatabaseInUseError, MisuseError } from "../errors.js";
import "./file/opfs-types.js";
import type { SyncFile } from "./file/sync-file.js";
import {
  DatabaseFile,
  DEFAULT_PAGE_SIZE,
  isValidPageSize,
  MAX_PAGE_SIZE,
  MIN_PAGE_SIZE,
} from "./pager/database-file.js";
import { Pager } from "./pager/pager.js";
import { ReadMarks } from "./pager/read-marks.js";
import { Wal } from "./pager/wal.js";

// ---------------------------------------------------------------------------
// Opening a database: its three files, recovery from the WAL, and the read
// marks it shares with other tabs. Opening is asynchronous because the
// browser's file and lock APIs are; everything after it is synchronous.
// ---------------------------------------------------------------------------

export interface StorageFiles {
  readonly database: SyncFile;
  readonly wal: SyncFile;
  /** The read-mark file; null when no other tab shares the database. */
  readonly marks: SyncFile | null;
}

export interface StorageOptions {
  /** The database's name, for file names in errors. */
  readonly name: string;
  /** Page size of a new database; an existing one keeps its own. */
  readonly pageSize?: number;
  /** WAL frames after which a checkpoint is due. */
  readonly checkpointThreshold?: number;
  /** Prefix of the Web Locks shared by the tabs that open these files. */
  readonly lockName?: string;
}

const DEFAULT_CHECKPOINT_THRESHOLD = 1000;

/**
 * Opens a database in its files, creating it if they are empty. Takes
 * ownership of the files: they are closed with the pager, or at once if
 * opening fails.
 */
export async function openPager(
  files: StorageFiles,
  options: StorageOptions,
): Promise<Pager> {
  const pageSize = options.pageSize ?? DEFAULT_PAGE_SIZE;
  if (!isValidPageSize(pageSize)) {
    throw new MisuseError(
      `page size ${String(pageSize)} is not a power of two from ${String(MIN_PAGE_SIZE)} to ${String(MAX_PAGE_SIZE)}`,
    );
  }
  let marks: ReadMarks | null = null;
  try {
    const { database, wal } = await whileHolding(
      options.lockName === undefined ? null : `${options.lockName}:open`,
      () => openFiles(files, options.name, pageSize),
    );
    if (files.marks !== null && options.lockName !== undefined) {
      marks = await ReadMarks.open(files.marks, options.lockName);
    }
    return new Pager(database, wal, {
      checkpointThreshold:
        options.checkpointThreshold ?? DEFAULT_CHECKPOINT_THRESHOLD,
      marks,
    });
  } catch (error) {
    if (marks === null) closeQuietly(files.marks);
    closeQuietly(files.wal);
    closeQuietly(files.database);
    throw error;
  }
}

/** Opens the OPFS files of database `name`. Only works in a dedicated worker. */
export async function openOpfsFiles(name: string): Promise<StorageFiles> {
  const root = await navigator.storage.getDirectory();
  const opened: SyncFile[] = [];
  try {
    for (const suffix of ["", "-wal", "-shm"]) {
      opened.push(await openShared(root, `${name}.opfsql${suffix}`));
    }
  } catch (error) {
    for (const file of opened) closeQuietly(file);
    throw isHeldByAnotherTab(error)
      ? new DatabaseInUseError(
          `database "${name}" is open in another tab, and this browser allows only one tab per database`,
          { cause: error },
        )
      : error;
  }
  const [database, wal, marks] = opened;
  if (database === undefined || wal === undefined || marks === undefined) {
    throw new Error("unreachable: three files were opened");
  }
  return { database, wal, marks };
}

function openFiles(
  files: StorageFiles,
  name: string,
  pageSize: number,
): { database: DatabaseFile; wal: Wal } {
  const fileName = `${name}.opfsql`;
  const database =
    files.database.getSize() === 0
      ? DatabaseFile.create(files.database, fileName, pageSize)
      : DatabaseFile.open(files.database, fileName);
  return {
    database,
    wal: Wal.open(files.wal, database.pageSize, `${fileName}-wal`),
  };
}

/** A file several tabs may hold at once, where the browser allows it. */
async function openShared(
  root: FileSystemDirectoryHandle,
  name: string,
): Promise<SyncFile> {
  const file = await root.getFileHandle(name, { create: true });
  return file.createSyncAccessHandle({ mode: "readwrite-unsafe" });
}

/**
 * How browsers without concurrent sync handles refuse a second one: Firefox
 * throws NoModificationAllowedError, WebKit InvalidStateError.
 */
function isHeldByAnotherTab(error: unknown): boolean {
  return (
    error instanceof DOMException &&
    (error.name === "NoModificationAllowedError" ||
      error.name === "InvalidStateError")
  );
}

/** Runs `work` while holding the Web Lock `name`, if there is one. */
async function whileHolding<T>(name: string | null, work: () => T): Promise<T> {
  return name === null ? work() : navigator.locks.request(name, work);
}

/** Cleanup after a failed open must not hide the error that caused it. */
function closeQuietly(file: SyncFile | null): void {
  try {
    file?.close();
  } catch {
    // The open already failed; its error is the one worth reporting.
  }
}
