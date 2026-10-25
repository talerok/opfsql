export class StorageError extends Error {
  override readonly name: string = "StorageError";

  constructor(message: string, options?: ErrorOptions) {
    super(`Storage error: ${message}`, options);
  }
}

/** A page does not fit the backend's page size. Nothing was written. */
export class PageOverflowError extends StorageError {
  override readonly name = "PageOverflowError";

  constructor(
    readonly pageNo: number,
    readonly size: number,
    readonly capacity: number,
  ) {
    super(`page ${pageNo} is ${size} bytes, exceeds page capacity ${capacity}`);
  }
}

/** A row is larger than a page can hold. Nothing was written. */
export class RowTooLargeError extends StorageError {
  override readonly name = "RowTooLargeError";

  constructor(
    readonly table: string,
    readonly size: number,
    readonly limit: number,
  ) {
    super(
      `row of table "${table}" is ${size} bytes, exceeds the limit of ${limit} bytes`,
    );
  }
}

/** An index key is larger than its index allows. Nothing was written. */
export class IndexKeyTooLargeError extends StorageError {
  override readonly name = "IndexKeyTooLargeError";

  constructor(
    readonly index: string,
    readonly size: number,
    readonly limit: number,
  ) {
    super(
      `key of index "${index}" is ${size} bytes, exceeds the limit of ${limit} bytes`,
    );
  }
}

/** The file was written in an on-disk format this version of opfsql does not read. */
export class UnsupportedFormatError extends StorageError {
  override readonly name = "UnsupportedFormatError";

  constructor(
    readonly file: string,
    readonly version: number,
    readonly supported: number,
  ) {
    super(
      `"${file}" uses file format ${version}, but this opfsql reads format ${supported}`,
    );
  }
}

/**
 * Another tab already has the database open, and this browser lets only one
 * tab open its files: only Chromium supports several tabs per database.
 */
export class DatabaseInUseError extends StorageError {
  override readonly name = "DatabaseInUseError";

  constructor(dbName: string, cause: unknown) {
    super(
      `database "${dbName}" is open in another tab, and this browser allows only one tab per database`,
      { cause },
    );
  }
}

/**
 * A failed WAL append could not be undone, so whether that transaction is
 * durable is unknown. The WAL refuses further commits; reopening the database
 * recovers whatever is on disk.
 */
export class WalUnknownStateError extends StorageError {
  override readonly name = "WalUnknownStateError";

  constructor(cause: unknown) {
    super(
      "WAL is in an unknown state after a failed write; reopen the database",
      { cause },
    );
  }
}
