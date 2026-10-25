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
  try {
    return await openPagerUnguarded(files, options);
  } catch (error) {
    closeQuietly(files.marks);
    closeQuietly(files.wal);
    closeQuietly(files.database);
    throw error;
  }
}

/** Opens the OPFS files of database `name`. Only works in a dedicated worker. */
export async function openOpfsFiles(name: string): Promise<StorageFiles> {
  const root = await navigator.storage.getDirectory();
  const opened: SyncFile[] = [];
  const open = async (suffix: string): Promise<SyncFile> => {
    const file = await openShared(root, fileName(name, suffix));
    opened.push(file);
    return file;
  };
  try {
    return {
      database: await open(""),
      wal: await open("-wal"),
      marks: await open("-shm"),
    };
  } catch (error) {
    for (const file of opened) closeQuietly(file);
    throw isHeldByAnotherTab(error) ? inUse(name, error) : error;
  }
}

/** openPager without the cleanup when it fails. */
async function openPagerUnguarded(
  files: StorageFiles,
  options: StorageOptions,
): Promise<Pager> {
  const pageSize = options.pageSize ?? DEFAULT_PAGE_SIZE;
  checkPageSize(pageSize);
  const { name, lockName } = options;
  const openLock = lockName === undefined ? null : `${lockName}:open`;
  const { database, wal } = await whileHolding(openLock, () =>
    openDatabaseAndWal(files, name, pageSize),
  );
  const marks = await openMarks(files.marks, lockName);
  const checkpointThreshold =
    options.checkpointThreshold ?? DEFAULT_CHECKPOINT_THRESHOLD;
  return new Pager(database, wal, { checkpointThreshold, marks });
}

function checkPageSize(pageSize: number): void {
  if (!isValidPageSize(pageSize)) {
    throw new MisuseError(
      `page size ${String(pageSize)} is not a power of two from ${String(MIN_PAGE_SIZE)} to ${String(MAX_PAGE_SIZE)}`,
    );
  }
}

/** Read marks are shared through their file and the tabs' Web Locks. */
async function openMarks(
  file: SyncFile | null,
  lockName: string | undefined,
): Promise<ReadMarks | null> {
  if (file === null) return null;
  if (lockName === undefined) {
    throw new MisuseError("a read-mark file needs a lock name");
  }
  return ReadMarks.open(file, lockName);
}

function inUse(name: string, cause: unknown): DatabaseInUseError {
  return new DatabaseInUseError(
    `database "${name}" is open in another tab, and this browser allows only one tab per database`,
    { cause },
  );
}

/** Recovers the WAL, creating the database first if its file is empty. */
function openDatabaseAndWal(
  files: StorageFiles,
  name: string,
  pageSize: number,
): { database: DatabaseFile; wal: Wal } {
  const database = openOrCreate(files.database, fileName(name), pageSize);
  const walName = fileName(name, "-wal");
  return { database, wal: Wal.open(files.wal, database.pageSize, walName) };
}

function openOrCreate(
  file: SyncFile,
  name: string,
  pageSize: number,
): DatabaseFile {
  if (file.getSize() === 0) return DatabaseFile.create(file, name, pageSize);
  return DatabaseFile.open(file, name);
}

/** The name of one of database `name`'s files: suffix "", "-wal" or "-shm". */
function fileName(name: string, suffix = ""): string {
  return `${name}.opfsql${suffix}`;
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
