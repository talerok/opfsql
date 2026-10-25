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
