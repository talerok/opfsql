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
// Where one engine's transactions begin and end: its pager, and the write
// lock it shares with the other tabs. A reader pins the latest commit; a
// writer takes the lock first, so it writes on top of the latest commit and
// no one commits under it, and ends by committing or rolling back here, so
// the lock is released, after a checkpoint if one is due, whatever failed.
// The pager reads other tabs' commits itself before each of these.
// ---------------------------------------------------------------------------

export interface TransactionManagerOptions {
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

export class TransactionManager {
  private readonly lock: WriteLock;
  /** Work still running after the call that started it: close() waits for it. */
  private readonly background = new Set<Promise<void>>();

  constructor(
    private readonly pager: Pager,
    private readonly options: TransactionManagerOptions,
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

  /** Commits the writer's transaction, and ends it as endWriter does. */
  commitWriter(writer: Writer): void {
    this.endWriter(writer, () => {
      writer.transaction.commit();
    });
  }

  /** Rolls back the writer's transaction, and ends it as endWriter does. */
  rollbackWriter(writer: Writer): void {
    this.endWriter(writer, () => {
      writer.transaction.rollback();
    });
  }

  /**
   * Copies the WAL into the database file as far as every tab's snapshots
   * allow, under the write lock so it cannot race a commit. Without waiting
   * when this engine holds the lock already: its writer cannot commit while
   * the engine runs this.
   */
  async checkpoint(signal: AbortSignal): Promise<void> {
    await this.underLock(signal, () => {
      this.pager.checkpoint();
    });
  }

  /**
   * Closes the files once background work has finished: a release of dead
   * tabs' read marks still running writes the read-mark file.
   */
  async close(): Promise<void> {
    await Promise.all(this.background);
    this.pager.close();
  }

  /**
   * Ends a write transaction with `end`, then checkpoints if the WAL has
   * grown enough — that needs the lock — then releases the lock. The lock
   * goes whatever failed, or every writer in every tab would wait for it.
   */
  private endWriter(writer: Writer, end: () => void): void {
    try {
      end();
    } finally {
      try {
        this.checkpointIfDue();
      } finally {
        writer.lease.release();
      }
    }
  }

  /** Lowers a stale checkpoint intent, under the lock. */
  private async resetStaleIntent(signal: AbortSignal): Promise<void> {
    await this.underLock(signal, () => {
      this.pager.resetStaleIntent();
    });
  }

  /**
   * Runs `work` under the write lock: at once when this engine holds it
   * already, or after taking it. Running at once is safe only because
   * everything below the engine is synchronous: nothing of the holder's
   * runs between this check and `work`.
   */
  private async underLock(
    signal: AbortSignal,
    work: () => void,
  ): Promise<void> {
    if (this.lock.isHeld) {
      work();
      return;
    }
    const lease = await this.acquire(signal);
    try {
      work();
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
    if (this.pager.isCheckpointDue()) this.clearDeadMarks();
  }

  /** In the background: the commit that got here has succeeded. */
  private clearDeadMarks(): void {
    const release = this.reportFailure(this.pager.clearDeadMarks());
    this.background.add(release);
    void this.forgetWhenDone(release);
  }

  private async forgetWhenDone(work: Promise<void>): Promise<void> {
    await work;
    this.background.delete(work);
  }

  private async reportFailure(work: Promise<void>): Promise<void> {
    try {
      await work;
    } catch (error) {
      this.options.onCheckpointError(toOpfsqlError(error));
    }
  }
}
