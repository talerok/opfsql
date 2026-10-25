import { CorruptError } from "../../errors.js";
import { BTreePageEditor } from "../format/btree-page.js";
import { readUint32, writeUint32 } from "../format/bytes.js";
import { checkPrefix, PREFIX_SIZE, writePrefix } from "../file/file-prefix.js";
import type { SyncFile } from "../file/sync-file.js";
import { newMapPage } from "./allocation.js";

// ---------------------------------------------------------------------------
// The database file (docs/file-format.md, Main file): pages of one size,
// page 0 the header. It holds the pages as of the last checkpoint; the WAL
// holds what was committed since.
// ---------------------------------------------------------------------------

export const MIN_PAGE_SIZE = 4096;
export const MAX_PAGE_SIZE = 65_536;
export const DEFAULT_PAGE_SIZE = 8192;

const PAGE_SIZE_OFFSET = PREFIX_SIZE;
const SCHEMA_VERSION_OFFSET = PREFIX_SIZE + 4;

export const HEADER_PAGE = 0;
const FIRST_MAP_PAGE = 1;
export const SCHEMA_ROOT_PAGE = 2;

export function isValidPageSize(pageSize: number): boolean {
  return (
    pageSize >= MIN_PAGE_SIZE &&
    pageSize <= MAX_PAGE_SIZE &&
    (pageSize & (pageSize - 1)) === 0
  );
}

/** The schema version recorded in a header page. */
export function schemaVersionOf(header: Uint8Array): number {
  return readUint32(header, SCHEMA_VERSION_OFFSET);
}

export function setSchemaVersion(header: Uint8Array, version: number): void {
  writeUint32(header, SCHEMA_VERSION_OFFSET, version);
}

export class DatabaseFile {
  private constructor(
    private readonly file: SyncFile,
    readonly name: string,
    readonly pageSize: number,
  ) {}

  /** Writes an empty database: the header, the first allocation map and an empty schema table. */
  static create(file: SyncFile, name: string, pageSize: number): DatabaseFile {
    const inUse = [HEADER_PAGE, SCHEMA_ROOT_PAGE];
    const pages = new Map([
      [HEADER_PAGE, newHeaderPage(pageSize)],
      [FIRST_MAP_PAGE, newMapPage(pageSize, FIRST_MAP_PAGE, inUse)],
      [SCHEMA_ROOT_PAGE, newSchemaRoot(pageSize)],
    ]);
    const database = new DatabaseFile(file, name, pageSize);
    file.truncate(0);
    database.write(pages);
    database.flush();
    return database;
  }

  /** Opens an existing database file, checking its header. */
  static open(file: SyncFile, name: string): DatabaseFile {
    const header = new Uint8Array(PREFIX_SIZE + 4);
    file.read(header, { at: 0 });
    checkPrefix(header, "database", name);
    const pageSize = readUint32(header, PAGE_SIZE_OFFSET);
    if (!isValidPageSize(pageSize)) {
      throw new CorruptError(
        `"${name}" has an invalid page size ${String(pageSize)}`,
      );
    }
    return new DatabaseFile(file, name, pageSize);
  }

  /** Pages in the file. */
  get pageCount(): number {
    return Math.floor(this.file.getSize() / this.pageSize);
  }

  read(pageNo: number): Uint8Array {
    const page = new Uint8Array(this.pageSize);
    const read = this.file.read(page, { at: pageNo * this.pageSize });
    if (read !== this.pageSize) {
      throw new CorruptError(
        `page ${String(pageNo)} is past the end of "${this.name}"`,
      );
    }
    return page;
  }

  /** Writes pages in place; flush() makes them durable. */
  write(pages: ReadonlyMap<number, Uint8Array>): void {
    for (const [pageNo, page] of pages) {
      this.file.write(page, { at: pageNo * this.pageSize });
    }
  }

  /** Shrinks or grows the file to `pageCount` pages. */
  resize(pageCount: number): void {
    this.file.truncate(pageCount * this.pageSize);
  }

  flush(): void {
    this.file.flush();
  }

  close(): void {
    this.file.close();
  }
}

function newHeaderPage(pageSize: number): Uint8Array {
  const header = new Uint8Array(pageSize);
  writePrefix(header, "database");
  writeUint32(header, PAGE_SIZE_OFFSET, pageSize);
  return header;
}

/** The schema table starts as one empty leaf. */
function newSchemaRoot(pageSize: number): Uint8Array {
  const page = new Uint8Array(pageSize);
  BTreePageEditor.initialize(page, "table-leaf");
  return page;
}
