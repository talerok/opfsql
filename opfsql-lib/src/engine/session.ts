import { MisuseError } from "../errors.js";
import type { Value } from "../values/index.js";
import type {
  CallOptions,
  Result,
  Schema,
  SessionContract,
  StatementContract,
  TransactionOptions,
} from "./api/index.js";
import type { SessionCore } from "./session-core.js";
import type { PreparedPlan } from "./statements.js";

/**
 * A connection to the database inside the worker: its own transaction, its
 * own snapshot. Calls run one at a time, in the order they were made. Close
 * it when done; one that is garbage-collected is closed by its engine.
 */
export class Session implements SessionContract, AsyncDisposable {
  /** Engine.connect() makes sessions. */
  constructor(private readonly core: SessionCore) {}

  private async rollbackQuietly(): Promise<void> {
    try {
      await this.rollback();
    } catch {
      // the error worth reporting is the one that made us roll back
    }
  }

  /** True between BEGIN and COMMIT or ROLLBACK, aborted or not. */
  get inTransaction(): boolean {
    return this.core.inTransaction;
  }

  /** Runs one statement. */
  async query(
    sql: string,
    parameters?: readonly Value[],
    options: CallOptions = {},
  ): Promise<Result> {
    return this.core.query(sql, parameters, options);
  }

  /** Runs a script of statements; see docs/api.md for how they commit. */
  async execute(script: string, options: CallOptions = {}): Promise<Result[]> {
    return this.core.execute(script, options);
  }

  /** Parses and binds one statement for repeated runs. */
  async prepare(
    sql: string,
    options: CallOptions = {},
  ): Promise<PreparedStatement> {
    const plan = await this.core.prepare(sql, options);
    return new PreparedStatement(this, this.core, plan);
  }

  /** BEGIN, deferred unless `options.mode` says otherwise. */
  async begin(options: TransactionOptions = {}): Promise<void> {
    await this.core.begin(options);
  }

  async commit(options: CallOptions = {}): Promise<void> {
    await this.core.commit(options);
  }

  async rollback(options: CallOptions = {}): Promise<void> {
    await this.core.rollback(options);
  }

  /**
   * Begins a transaction (immediate unless `options.mode` says otherwise),
   * runs `work` and commits; rolls back and rethrows if `work` throws.
   */
  async transaction<T>(
    work: (session: Session) => Promise<T> | T,
    options: TransactionOptions = {},
  ): Promise<T> {
    await this.begin({ ...options, mode: options.mode ?? "immediate" });
    let value: T;
    try {
      value = await work(this);
    } catch (error) {
      await this.rollbackQuietly();
      throw error;
    }
    await this.commit();
    return value;
  }

  /** The schema this session sees. */
  async schema(options: CallOptions = {}): Promise<Schema> {
    return this.core.schema(options);
  }

  async close(): Promise<void> {
    await this.core.close();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}

/**
 * A statement parsed and bound once. When the schema changes it binds again
 * before its next run; it never goes stale.
 */
export class PreparedStatement implements StatementContract, Disposable {
  private isClosed = false;

  constructor(
    /** The session it runs on; kept alive while the statement is. */
    readonly session: Session,
    private readonly core: SessionCore,
    private readonly plan: PreparedPlan,
  ) {}

  /** Result column names as of the last bind; empty for writes. */
  get columns(): readonly string[] {
    return this.plan.columns;
  }

  async query(
    parameters?: readonly Value[],
    options: CallOptions = {},
  ): Promise<Result> {
    if (this.isClosed) throw new MisuseError("the statement is closed");
    return this.core.runPrepared(this.plan, parameters, options);
  }

  close(): void {
    this.isClosed = true;
  }

  [Symbol.dispose](): void {
    this.close();
  }
}
