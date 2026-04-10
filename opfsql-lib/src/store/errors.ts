export class StorageError extends Error {
  override readonly name = 'StorageError';

  constructor(message: string, cause?: unknown) {
    super(message, { cause });
  }
}

const STORAGE_ERROR_MESSAGES: Record<string, string> = {
  QuotaExceededError: 'Storage quota exceeded',
  InvalidStateError: 'Database connection is invalid',
  DataError: 'Invalid key or value',
  DataCloneError: 'Value cannot be cloned for storage',
};

export function wrapStorageError(err: unknown): StorageError {
  if (err instanceof StorageError) return err;
  if (err instanceof DOMException) {
    const message = STORAGE_ERROR_MESSAGES[err.name] ?? err.message;
    return new StorageError(message, err);
  }
  if (err instanceof Error) {
    return new StorageError(err.message, err);
  }
  return new StorageError(String(err));
}
