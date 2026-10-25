import { SerializationError } from "../errors.js";
import { SyncIndexManager } from "../store/index-manager.js";
import type { SessionStore } from "../store/session-store.js";
import type { Value } from "../values/index.js";
import type { Result, TransactionMode } from "./api/index.js";
import { RecordingTableManager } from "./rows.js";
import {
  type SchemaLoader,
  stageSchema,
  type VersionedSchema,
  withChanges,
} from "./schema.js";
import { abortErrorOf } from "./signals.js";
import { runBound, type Step } from "./statements.js";
import type { Lease, WriteLock } from "./write-lock.js";

/** What a transaction needs from its engine. */
export interface TransactionHost {
  readonly busyTimeout: number;
  readonly lock: WriteLock;
  readonly schemas: SchemaLoader;
  /**
   * Pins the latest commit in `store` and returns its position, catching up
   * with other tabs first unless this engine holds the write lock.
   */
  pin(store: SessionStore, signal: AbortSignal): Promise<number>;
  latestSchemaVersion(): number;
  /** Deferred maintenance (checkpoints), run under the lock after a commit. */
  maintain(): void;
}

/** Where a transaction runs: its session's store, its engine, and who holds the lock. */
export interface TransactionContext {
  readonly store: SessionStore;
  readonly host: TransactionHost;
  readonly owner: object;
}

/**
 * A unit of work: one pinned snapshot, the schema stored in it, and the write
 * lock once it writes. A statement outside BEGIN runs in one of its own.
 */
export class Transaction {
  /**
   * Starts a transaction. An immediate one takes the lock first, so the
   * snapshot it pins — and the schema read from it — is the latest commit,
   * and stays so.
   */
  static async begin(
    context: TransactionContext,
    mode: TransactionMode,
    signal: AbortSignal,
  ): Promise<Transaction> {
    const lease = mode === "immediate" ? await acquire(context, signal) : null;
    try {
      const position = await context.host.pin(context.store, signal);
      const schema = context.host.schemas.load(context.store, position);
      return new Transaction(context, schema, lease);
    } catch (error) {
      try {
        context.store.endSnapshot();
      } finally {
        lease?.release();
      }
      throw error;
    }
  }

  private state: "active" | "aborted" = "active";
  /** `base`, or `base` with this transaction's own DDL. */
  private working: VersionedSchema;

  private constructor(
    private readonly context: TransactionContext,
    /** The schema as of the snapshot. */
    private readonly base: VersionedSchema,
    private lease: Lease | null,
  ) {
    this.working = base;
  }

  private discard(): void {
    try {
      this.context.store.rollback();
    } finally {
      this.release();
    }
  }

  /**
   * Maintenance runs after the snapshot is gone and before the lock is; the
   * lock goes whatever fails before it, or every writer in every tab would
   * wait for it until this tab died.
   */
  private release(): void {
    const lease = this.lease;
    this.lease = null;
    try {
      this.context.store.endSnapshot();
      if (lease !== null) this.context.host.maintain();
    } finally {
      lease?.release();
    }
  }

  /** Aborted: its work is discarded, its lock and snapshot released. */
  get isAborted(): boolean {
    return this.state === "aborted";
  }

  get schema(): VersionedSchema {
    return this.working;
  }

  /**
   * Takes the lock for a deferred transaction's first write. Writing on a
   * snapshot that another commit has overtaken could lose that commit, so
   * that fails instead.
   */
  async makeWritable(signal: AbortSignal): Promise<void> {
    if (this.lease !== null) return;
    this.lease = await acquire(this.context, signal);
    const hasSchemaMoved =
      this.context.host.latestSchemaVersion() !== this.base.version;
    if (hasSchemaMoved || this.context.store.isSnapshotStale()) {
      throw new SerializationError();
    }
  }

  run(step: Step, parameters: readonly Value[] | undefined): Result {
    const { catalog } = this.working;
    const { store } = this.context;
    const target = {
      catalog,
      rows: new RecordingTableManager(store, () => catalog),
      indexes: new SyncIndexManager(store, () => catalog),
    };
    const outcome = runBound(step.bind(catalog), target, parameters);
    if (outcome.changes.length > 0) {
      this.working = {
        version: this.base.version,
        catalog: withChanges(catalog, outcome.changes),
      };
    }
    return outcome.result;
  }

  /** Commits; if that fails, nothing of the transaction remains. */
  commit(): void {
    const isSchemaChanged = this.working !== this.base;
    const version = this.base.version + 1;
    try {
      if (isSchemaChanged) {
        stageSchema(this.context.store, this.working.catalog, version);
      }
      this.context.store.commit();
    } catch (error) {
      this.discard();
      throw error;
    }
    if (isSchemaChanged) {
      this.context.host.schemas.remember({
        version,
        catalog: this.working.catalog,
      });
    }
    this.release();
  }

  rollback(): void {
    if (this.state === "active") this.discard();
  }

  /** A failed statement aborts its transaction and frees what it holds at once. */
  abort(): void {
    if (this.state === "aborted") return;
    this.discard();
    this.state = "aborted";
  }
}

async function acquire(
  context: TransactionContext,
  signal: AbortSignal,
): Promise<Lease> {
  const lease = await context.host.lock.acquire(context.owner, {
    signal,
    timeout: context.host.busyTimeout,
  });
  if (signal.aborted) {
    lease.release();
    throw abortErrorOf(signal);
  }
  return lease;
}
