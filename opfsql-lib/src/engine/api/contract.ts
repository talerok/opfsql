import type { OpfsqlError } from "../../errors.js";
import type { Value } from "../../values/index.js";
import type { Result } from "./result.js";
import type { Schema } from "./schema.js";

// The engine's contract: what Engine, Session and PreparedStatement promise,
// and all the worker host relies on.

/** Options every call takes. */
export interface CallOptions {
  /** Aborting it cancels the call while it waits; a running call finishes. */
  readonly signal?: AbortSignal;
}

/**
 * "deferred" takes the write lock at the first write, "immediate" before the
 * snapshot, so an immediate transaction never fails to serialize.
 */
export type TransactionMode = "deferred" | "immediate";

export interface TransactionOptions extends CallOptions {
  readonly mode?: TransactionMode;
}

/** How a database is opened. */
export interface OpenOptions {
  /** Milliseconds a write waits for the write lock before BUSY. Default 5000. */
  readonly busyTimeout?: number;
  /** Page size of a new database, a power of two from 4096 to 65536. Default 8192. */
  readonly pageSize?: number;
  /** WAL frames after which a commit triggers a checkpoint. Default 1000. */
  readonly checkpointThreshold?: number;
  /**
   * Called when a background checkpoint fails; the commit that triggered it
   * has succeeded. Default: console.warn.
   */
  readonly onCheckpointError?: (error: OpfsqlError) => void;
}

export interface SessionOptions {
  /**
   * Every call must end the transactions it begins: one left open is rolled
   * back and the call fails with MISUSE. For sessions shared by unrelated
   * callers, which must never find themselves inside another's transaction.
   */
  readonly callScopedTransactions?: boolean;
}

export interface StatementContract {
  /** Result column names as of the last plan; empty for writes. */
  readonly columns: readonly string[];
  query(parameters?: readonly Value[], options?: CallOptions): Promise<Result>;
  close(): void;
}

export interface SessionContract {
  query(
    sql: string,
    parameters?: readonly Value[],
    options?: CallOptions,
  ): Promise<Result>;
  execute(script: string, options?: CallOptions): Promise<Result[]>;
  prepare(sql: string, options?: CallOptions): Promise<StatementContract>;
  begin(options?: TransactionOptions): Promise<void>;
  commit(options?: CallOptions): Promise<void>;
  rollback(options?: CallOptions): Promise<void>;
  schema(options?: CallOptions): Promise<Schema>;
  close(): Promise<void>;
}

export interface EngineContract {
  connect(options?: SessionOptions): SessionContract;
  close(): Promise<void>;
}

/** Opens the engine a worker serves. */
export type EngineOpener = (
  name: string,
  options: OpenOptions,
) => Promise<EngineContract>;
