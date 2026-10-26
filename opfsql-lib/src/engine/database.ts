import {
  type OpfsqlError,
  SerializationError,
  toOpfsqlError,
} from "../errors.js";
import type {
  Pager,
  ReadTransaction,
  WriteTransaction,
} from "../storage/index.js";
import { abortErrorOf } from "../async/index.js";
import { type Lease, WriteLock } from "./write-lock.js";

// ---------------------------------------------------------------------------
// One engine's database: its pager, and the write lock it shares with the
// other tabs. Transactions begin and end here. A reader pins the latest
// commit; a writer takes the lock first, so it writes on top of the latest
// commit and no one commits under it. The pager reads other tabs' commits
// itself before each of these.
// ---------------------------------------------------------------------------

export interface DatabaseOptions {
  /** Prefix of the Web Locks the engines sharing these files use. */
  readonly lockName: string;
  /** Milliseconds a writer waits for the lock before BUSY. */
  readonly busyTimeout: number;
  /** Told when a checkpoint after a commit fails; the commit stands. */
  readonly onCheckpointError: (error: OpfsqlError) => void;
}

/** A write transaction and the lock it holds. */
export interface Writer {
  readonly transaction: WriteTransaction;
  readonly lease: Lease;
}

export class Database {
  private readonly lock: WriteLock;

  constructor(
    private readonly pager: Pager,
    private readonly options: DatabaseOptions,
  ) {
    this.lock = new WriteLock(`${options.lockName}:write`);
  }

  /**
   * Pins the latest commit. The pager refuses only while a checkpoint
   * intent no commit backs stands: one left by an earlier database of the
   * same name. Lower it under the lock and pin again.
   */
  async beginRead(signal: AbortSignal): Promise<ReadTransaction> {
    for (;;) {
      const read = this.pager.beginRead();
      if (read !== null) return read;
      await this.resetStaleIntent(signal);
    }
  }

  /** Takes the write lock and begins writing on the latest commit. */
  async beginWrite(signal: AbortSignal): Promise<Writer> {
    const lease = await this.acquire(signal);
    try {
      return { transaction: this.pager.beginWrite(), lease };
    } catch (error) {
      lease.release();
      throw error;
    }
  }

  /**
   * Takes the write lock for a transaction that has read `read` so far. If
   * another writer has committed since, what it read may be stale, and it
   * fails with SERIALIZATION_FAILURE instead. Otherwise it writes on the
   * same snapshot, pinned before `read` ends so it is never released.
   */
  async upgrade(read: ReadTransaction, signal: AbortSignal): Promise<Writer> {
    const writer = await this.beginWrite(signal);
    if (writer.transaction.snapshot === read.snapshot) {
      read.end();
      return writer;
    }
    writer.transaction.rollback();
    writer.lease.release();
    throw new SerializationError();
  }

  /**
   * After a write transaction has ended: checkpoints if the WAL has grown
   * enough — that needs the lock — then releases the lock. The lock goes
   * whatever failed, or every writer in every tab would wait for it.
   */
  endWrite(writer: Writer): void {
    try {
      this.checkpointIfDue();
    } finally {
      writer.lease.release();
    }
  }

  /**
   * Copies the WAL into the database file as far as every tab's snapshots
   * allow, under the write lock so it cannot race a commit. Without waiting
   * when this engine holds the lock already: its writer cannot commit while
   * the engine runs this.
   */
  async checkpoint(signal: AbortSignal): Promise<void> {
    if (this.lock.isHeld) {
      this.pager.checkpoint();
      return;
    }
    const lease = await this.acquire(signal);
    try {
      this.pager.checkpoint();
    } finally {
      lease.release();
    }
  }

  close(): void {
    this.pager.close();
  }

  /** Lowers a stale checkpoint intent, under the lock. */
  private async resetStaleIntent(signal: AbortSignal): Promise<void> {
    if (this.lock.isHeld) {
      this.pager.resetStaleIntent();
      return;
    }
    const lease = await this.acquire(signal);
    try {
      this.pager.resetStaleIntent();
    } finally {
      lease.release();
    }
  }

  private async acquire(signal: AbortSignal): Promise<Lease> {
    const lease = await this.lock.acquire({
      signal,
      timeout: this.options.busyTimeout,
    });
    if (signal.aborted) {
      lease.release();
      throw abortErrorOf(signal);
    }
    return lease;
  }

  private checkpointIfDue(): void {
    if (!this.pager.isCheckpointDue()) return;
    try {
      this.pager.checkpoint();
    } catch (error) {
      this.options.onCheckpointError(toOpfsqlError(error));
      return;
    }
    // Still due: a snapshot holds it back. If a tab that died holds it,
    // free its read mark so the next checkpoint can go further.
    if (this.pager.isCheckpointDue()) this.releaseAbandonedSnapshots();
  }

  /** In the background: the commit that got here has succeeded. */
  private releaseAbandonedSnapshots(): void {
    void this.reportFailure(this.pager.releaseAbandonedSnapshots());
  }

  private async reportFailure(work: Promise<void>): Promise<void> {
    try {
      await work;
    } catch (error) {
      this.options.onCheckpointError(toOpfsqlError(error));
    }
  }
}
