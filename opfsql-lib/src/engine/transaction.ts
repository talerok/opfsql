import { InternalError, SerializationError } from "../errors.js";
import type { CatalogChange } from "../executor/types.js";
import { SyncIndexManager } from "../store/index-manager.js";
import type { SessionStore } from "../store/session-store.js";
import type { Value } from "../values/index.js";
import type { Result, TransactionMode } from "./api/index.js";
import { RecordingTableManager } from "./rows.js";
import {
  type SchemaLoader,
  stageSchemaChanges,
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
  /** Copies the WAL into the database file when the WAL asks for it. */
  checkpointIfNeeded(): void;
}

/** Where a transaction runs: its session's store and its engine. */
export interface TransactionContext {
  readonly store: SessionStore;
  readonly host: TransactionHost;
}

/**
 * Active until committed or rolled back (ended); aborted by a failed
 * statement, holding nothing, until rolled back.
 */
type TransactionState = "active" | "aborted" | "ended";

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

  private state: TransactionState = "active";
  /** `base`, or `base` with this transaction's own DDL. */
  private working: VersionedSchema;
  /** This transaction's DDL in order: what its commit stores. */
  private readonly changes: CatalogChange[] = [];

  private constructor(
    private readonly context: TransactionContext,
    /** The schema as of the snapshot. */
    private readonly base: VersionedSchema,
    private lease: Lease | null,
  ) {
    this.working = base;
  }

  private assertActive(): void {
    if (this.state !== "active") {
      throw new InternalError(`the transaction is ${this.state}`);
    }
  }

  /** The schema version this transaction's commit stores, if it changes it. */
  private get nextVersion(): number {
    return this.base.version + 1;
  }

  /** One commit: the schema changes staged with the pages. */
  private write(): void {
    if (this.changes.length > 0) {
      stageSchemaChanges(this.context.store, this.changes, this.nextVersion);
    }
    this.context.store.commit();
  }

  /** The schema just stored is the working one: keep it for others to reuse. */
  private rememberSchema(): void {
    if (this.changes.length === 0) return;
    const schema = { version: this.nextVersion, catalog: this.working.catalog };
    this.context.host.schemas.remember(schema);
  }

  private discard(): void {
    try {
      this.context.store.rollback();
    } finally {
      this.releaseWithCheckpoint();
    }
  }

  /**
   * Releases the snapshot, then checkpoints if the WAL asks for it — that
   * needs the lock, and no snapshot of ours holding it back — then releases
   * the lock. The lock goes whatever failed before it, or every writer in
   * every tab would wait for it until this tab died.
   */
  private releaseWithCheckpoint(): void {
    const lease = this.lease;
    this.lease = null;
    try {
      this.context.store.endSnapshot();
      if (lease !== null) this.context.host.checkpointIfNeeded();
    } finally {
      lease?.release();
    }
  }

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
    this.assertActive();
    if (this.lease !== null) return;
    this.lease = await acquire(this.context, signal);
    if (this.context.store.isSnapshotStale()) throw new SerializationError();
  }

  run(step: Step, parameters: readonly Value[] | undefined): Result {
    this.assertActive();
    const { catalog } = this.working;
    const { store } = this.context;
    const target = {
      catalog,
      rows: new RecordingTableManager(store, () => catalog),
      indexes: new SyncIndexManager(store, () => catalog),
    };
    const outcome = runBound(step.bind(catalog), target, parameters);
    if (outcome.changes.length > 0) {
      this.changes.push(...outcome.changes);
      const changed = withChanges(catalog, outcome.changes);
      this.working = { version: this.base.version, catalog: changed };
    }
    return outcome.result;
  }

  /** Commits; if that fails, nothing of the transaction remains. */
  commit(): void {
    this.assertActive();
    this.state = "ended";
    try {
      this.write();
    } catch (error) {
      this.discard();
      throw error;
    }
    this.rememberSchema();
    this.releaseWithCheckpoint();
  }

  rollback(): void {
    const wasActive = this.state === "active";
    this.state = "ended";
    if (wasActive) this.discard();
  }

  /** A failed statement aborts its transaction and frees what it holds at once. */
  abort(): void {
    if (this.state !== "active") return;
    this.state = "aborted";
    this.discard();
  }
}

async function acquire(
  context: TransactionContext,
  signal: AbortSignal,
): Promise<Lease> {
  const lease = await context.host.lock.acquire({
    signal,
    timeout: context.host.busyTimeout,
  });
  if (signal.aborted) {
    lease.release();
    throw abortErrorOf(signal);
  }
  return lease;
}
