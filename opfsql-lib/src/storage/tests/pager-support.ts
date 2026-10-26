import { MemoryFile } from "../file/sync-file.js";
import type { StorageFiles } from "../open.js";
import type { Pager } from "../pager/pager.js";
import type {
  ReadTransaction,
  WriteTransaction,
} from "../pager/transaction.js";

// Transactions for tests, which fail loudly instead of returning null.

export function memoryFiles(): StorageFiles {
  return { database: new MemoryFile(), wal: new MemoryFile(), marks: null };
}

export function beginRead(pager: Pager): ReadTransaction {
  const transaction = pager.beginRead();
  if (transaction === null) throw new Error("no snapshot");
  return transaction;
}

/** Runs `change` in a write transaction and commits it. */
export function write<T>(
  pager: Pager,
  change: (transaction: WriteTransaction) => T,
): T {
  const transaction = pager.beginWrite();
  const result = change(transaction);
  transaction.commit();
  return result;
}

/** Runs `query` in a read transaction and ends it. */
export function read<T>(
  pager: Pager,
  query: (transaction: ReadTransaction) => T,
): T {
  const transaction = beginRead(pager);
  try {
    return query(transaction);
  } finally {
    transaction.end();
  }
}
