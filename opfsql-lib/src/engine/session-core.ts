import { AbortError, MisuseError, TransactionAbortedError } from "../errors.js";
import {
  type Statement,
  StatementType,
  type TransactionStatement,
  TransactionType,
} from "../sql/parser/index.js";
import type { SessionStore } from "../store/session-store.js";
import type { Value } from "../values/index.js";
import {
  type CallOptions,
  emptyResult,
  type Result,
  type Schema,
  type SessionOptions,
  type TransactionMode,
  type TransactionOptions,
} from "./api/index.js";
import { describeSchema } from "./describe-schema.js";
import type { VersionedSchema } from "./schema.js";
import { linkSignals } from "./signals.js";
import { bindAhead, eachStep, StepFailure } from "./script.js";
import { beginMode, checkParameters, parseScript, Step } from "./statements.js";
import { TaskQueue } from "./task-queue.js";
import {
  Transaction,
  type TransactionContext,
  type TransactionHost,
} from "./transaction.js";
import { locateStatement, toOpfsqlError } from "./translate-error.js";

/** What a session needs from its engine. */
export interface SessionHost extends TransactionHost {
  createStore(): SessionStore;
  /** The session has closed. */
  forget(session: SessionCore): void;
}

type ParameterValues = readonly Value[] | undefined;

/**
 * A session's state and rules; Session is the handle callers hold. Calls run
 * one at a time through the queue. Every statement reads the snapshot of its
 * transaction and the schema stored in that snapshot; a writer takes the
 * lock before it pins anything it will write on.
 */
export class SessionCore {
  private state: "open" | "closing" | "closed" = "open";

  private readonly queue = new TaskQueue();

  private readonly lifetime = new AbortController();

  private readonly context: TransactionContext;

  private transaction: Transaction | null = null;

  private closing: Promise<void> | null = null;

  constructor(
    private readonly host: SessionHost,
    private readonly options: SessionOptions,
  ) {
    this.context = { store: host.createStore(), host };
  }

  /** True between BEGIN and COMMIT or ROLLBACK, aborted or not. */
  get inTransaction(): boolean {
    return this.transaction !== null;
  }

  async query(
    sql: string,
    parameters: ParameterValues,
    options: CallOptions,
  ): Promise<Result> {
    return this.call(options, async (signal) => {
      const statement = onlyStatement(parseScript(sql), "query()");
      return this.runOne(new Step(sql, statement), parameters, signal);
    });
  }

  /** Runs a script; the error of a failed statement says which it was. */
  async execute(script: string, options: CallOptions): Promise<Result[]> {
    return this.call(options, async (signal) => {
      const steps = parseScript(script).map(
        (statement) => new Step(script, statement),
      );
      try {
        return await this.runScript(steps, signal);
      } catch (error) {
        if (!(error instanceof StepFailure)) throw error;
        throw locateStatement(toOpfsqlError(error.failure), error.index);
      }
    });
  }

  /** Parses and binds `sql`, so a statement that cannot run fails here. */
  async prepare(sql: string, options: CallOptions): Promise<Step> {
    return this.call(options, async (signal) => {
      const step = new Step(sql, onlyStatement(parseScript(sql), "prepare()"));
      if (step.kind !== "transaction") {
        await this.withSchema(signal, (schema) => step.bind(schema.catalog));
      }
      return step;
    });
  }

  async runPrepared(
    step: Step,
    parameters: ParameterValues,
    options: CallOptions,
  ): Promise<Result> {
    return this.call(options, async (signal) =>
      this.runOne(step, parameters, signal),
    );
  }

  async begin(options: TransactionOptions): Promise<void> {
    if (this.options.callScopedTransactions === true) {
      throw new MisuseError(
        "this session cannot keep a transaction open between calls; use a connection of its own",
      );
    }
    await this.call(options, async (signal) => {
      await this.beginNow(options.mode ?? "deferred", signal);
    });
  }

  async commit(options: CallOptions): Promise<void> {
    await this.call(options, () => {
      this.commitNow();
    });
  }

  async rollback(options: CallOptions): Promise<void> {
    await this.call(options, () => {
      this.rollbackNow();
    });
  }

  async schema(options: CallOptions): Promise<Schema> {
    return this.call(options, async (signal) =>
      this.withSchema(signal, (schema) => describeSchema(schema.catalog)),
    );
  }

  /**
   * Stops taking calls, cancels the ones waiting, lets a running one finish,
   * rolls back the open transaction and releases the lock and snapshot.
   */
  async close(): Promise<void> {
    this.closing ??= this.shutdown();
    return this.closing;
  }

  private async shutdown(): Promise<void> {
    this.state = "closing";
    this.lifetime.abort(new AbortError("the session was closed"));
    await this.queue.idle();
    this.transaction?.rollback();
    this.transaction = null;
    this.state = "closed";
    this.host.forget(this);
  }

  /** Runs `task` in turn, with the call's signal linked to the session's. */
  private async call<T>(
    options: CallOptions,
    task: (signal: AbortSignal) => Promise<T> | T,
  ): Promise<T> {
    if (this.state !== "open") throw new MisuseError("the session is closed");
    const linked = linkSignals([options.signal, this.lifetime.signal]);
    try {
      return await this.queue.run(
        async () => this.inTurn(task, linked.signal),
        linked.signal,
      );
    } catch (error) {
      throw toOpfsqlError(error);
    } finally {
      linked.dispose();
    }
  }

  /** One call's turn: its task, then no transaction left where none may stay. */
  private async inTurn<T>(
    task: (signal: AbortSignal) => Promise<T> | T,
    signal: AbortSignal,
  ): Promise<T> {
    const value = await task(signal);
    this.rejectLeftoverTransaction();
    return value;
  }

  /**
   * A session shared by unrelated callers keeps no transaction past a call:
   * one left open is rolled back, and the call fails with MISUSE.
   */
  private rejectLeftoverTransaction(): void {
    const transaction = this.transaction;
    const isLeftOver =
      transaction !== null && this.options.callScopedTransactions === true;
    if (!isLeftOver) return;
    this.transaction = null;
    transaction.rollback();
    throw new MisuseError(
      "a call on a shared connection must end the transaction it begins; it was rolled back",
    );
  }

  // -------------------------------------------------------------------------
  // Running statements
  // -------------------------------------------------------------------------

  /**
   * A script: outside a transaction, and controlling none, it runs as one
   * transaction; otherwise its statements run one by one, and a failure
   * rolls back a transaction the script began. Failures are StepFailures.
   */
  private async runScript(
    steps: readonly Step[],
    signal: AbortSignal,
  ): Promise<Result[]> {
    const isOneTransaction =
      this.transaction === null &&
      steps.every((step) => step.kind !== "transaction");
    if (isOneTransaction) return this.runAsOne(steps, signal);
    return this.runInOrder(steps, signal);
  }

  private async runAsOne(
    steps: readonly Step[],
    signal: AbortSignal,
  ): Promise<Result[]> {
    if (steps.length === 0) return [];
    const transaction = await this.beginForScript(steps, signal);
    try {
      const results = await eachStep(steps, (step) =>
        transaction.run(step, undefined),
      );
      transaction.commit();
      return results;
    } catch (error) {
      transaction.rollback();
      throw error;
    }
  }

  private async runInOrder(
    steps: readonly Step[],
    signal: AbortSignal,
  ): Promise<Result[]> {
    const before = this.transaction;
    try {
      return await eachStep(steps, async (step) =>
        this.runOne(step, undefined, signal),
      );
    } catch (error) {
      this.endBegunSince(before);
      throw error;
    }
  }

  /**
   * A script that writes takes the lock before its first statement. Its
   * statements are bound first, against the latest schema, as far as they
   * can be — up to the first DDL, which may change what later ones bind
   * against — so one that cannot bind fails without waiting for the lock.
   */
  private async beginForScript(
    steps: readonly Step[],
    signal: AbortSignal,
  ): Promise<Transaction> {
    const isWriting = steps.some((step) => step.kind === "write");
    if (isWriting) {
      await this.withLatestSchema(signal, ({ catalog }) => {
        bindAhead(steps, catalog);
      });
    }
    const mode = isWriting ? "immediate" : "deferred";
    return Transaction.begin(this.context, mode, signal);
  }

  /** One statement, as if sent alone: query(), a prepared statement, a script's step. */
  private async runOne(
    step: Step,
    parameters: ParameterValues,
    signal: AbortSignal,
  ): Promise<Result> {
    const statement = step.statement;
    if (isControl(statement)) {
      await this.control(statement, signal);
      return emptyResult;
    }
    const transaction = this.transaction;
    if (transaction === null) return this.runAlone(step, parameters, signal);
    return this.runInTransaction(transaction, step, parameters, signal);
  }

  /**
   * A statement in a transaction of its own. A write is bound against the
   * latest schema before it waits for the lock, so one that cannot bind
   * fails at once; the bind after the lock is the one that counts.
   */
  private async runAlone(
    step: Step,
    parameters: ParameterValues,
    signal: AbortSignal,
  ): Promise<Result> {
    const isWrite = step.kind === "write";
    if (isWrite) {
      await this.withLatestSchema(signal, ({ catalog }) => {
        checkParameters(step.bind(catalog), parameters);
      });
    }
    const mode = isWrite ? "immediate" : "deferred";
    const transaction = await Transaction.begin(this.context, mode, signal);
    let result: Result;
    try {
      result = transaction.run(step, parameters);
    } catch (error) {
      transaction.rollback();
      throw error;
    }
    transaction.commit();
    return result;
  }

  /**
   * A statement in the open transaction. Its first write takes the lock —
   * after the statement has bound, so a statement that cannot bind never
   * waits. Any failure aborts the transaction.
   */
  private async runInTransaction(
    transaction: Transaction,
    step: Step,
    parameters: ParameterValues,
    signal: AbortSignal,
  ): Promise<Result> {
    if (transaction.isAborted) throw new TransactionAbortedError();
    try {
      if (step.kind === "write") {
        checkParameters(step.bind(transaction.schema.catalog), parameters);
        await transaction.makeWritable(signal);
      }
      return transaction.run(step, parameters);
    } catch (error) {
      transaction.abort();
      throw error;
    }
  }

  // -------------------------------------------------------------------------
  // Transactions
  // -------------------------------------------------------------------------

  private async control(
    statement: TransactionStatement,
    signal: AbortSignal,
  ): Promise<void> {
    switch (statement.transactionType) {
      case TransactionType.BEGIN: {
        await this.beginNow(beginMode(statement), signal);
        return;
      }
      case TransactionType.COMMIT: {
        this.commitNow();
        return;
      }
      case TransactionType.ROLLBACK: {
        this.rollbackNow();
        return;
      }
    }
  }

  private async beginNow(
    mode: TransactionMode,
    signal: AbortSignal,
  ): Promise<void> {
    if (this.transaction !== null) {
      throw new MisuseError("a transaction is already open");
    }
    this.transaction = await Transaction.begin(this.context, mode, signal);
  }

  private commitNow(): void {
    const transaction = this.transaction;
    if (transaction === null) return;
    this.transaction = null;
    if (transaction.isAborted) {
      transaction.rollback();
      throw new TransactionAbortedError(
        "the transaction was aborted by an earlier error; COMMIT rolled it back",
      );
    }
    transaction.commit();
  }

  private rollbackNow(): void {
    this.transaction?.rollback();
    this.transaction = null;
  }

  /** Rolls back a transaction begun after `before`. */
  private endBegunSince(before: Transaction | null): void {
    const transaction = this.transaction;
    if (transaction === null || transaction === before) return;
    this.transaction = null;
    transaction.rollback();
  }

  /** Runs `read` on the schema this session sees: its transaction's, or the latest. */
  private async withSchema<T>(
    signal: AbortSignal,
    read: (schema: VersionedSchema) => T,
  ): Promise<T> {
    const transaction = this.transaction;
    if (transaction?.isAborted === true) throw new TransactionAbortedError();
    if (transaction !== null) return read(transaction.schema);
    return this.withLatestSchema(signal, read);
  }

  /** Runs `read` on the latest committed schema, pinned for the moment. */
  private async withLatestSchema<T>(
    signal: AbortSignal,
    read: (schema: VersionedSchema) => T,
  ): Promise<T> {
    const { store } = this.context;
    const position = await this.host.pin(store, signal);
    try {
      return read(this.host.schemas.load(store, position));
    } finally {
      store.endSnapshot();
    }
  }
}

function isControl(statement: Statement): statement is TransactionStatement {
  return statement.type === StatementType.TRANSACTION_STATEMENT;
}

function onlyStatement(
  statements: readonly Statement[],
  what: string,
): Statement {
  const [statement, ...rest] = statements;
  if (statements.length === 0 || rest.length > 0) {
    throw new MisuseError(
      `${what} takes exactly one statement; run scripts with execute()`,
    );
  }
  return statement;
}
