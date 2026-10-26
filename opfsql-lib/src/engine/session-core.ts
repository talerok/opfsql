import { linkSignals, TaskQueue } from "../async/index.js";
import { AbortError, MisuseError, toOpfsqlError } from "../errors.js";
import type { Schema as SchemaModel } from "../schema/index.js";
import {
  type Statement,
  StatementType,
  type TransactionStatement,
  TransactionType,
} from "../sql/parser/index.js";
import {
  NO_PARAMETERS,
  type ParameterValues,
  parametersOf,
  type Value,
} from "../values/index.js";
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
import { eachStep, planAhead, StepFailure } from "./script.js";
import { beginMode, checkParameters, parseScript, Step } from "./statements.js";
import { Transaction, type TransactionHost } from "./transaction.js";
import { locateStatement } from "./locate-statement.js";

/** What a session needs from its engine. */
export interface SessionHost extends TransactionHost {
  /** The session has closed. */
  forget(session: SessionCore): void;
}

/**
 * A session's state and rules; Session is the handle callers hold. Calls run
 * one at a time through the queue. Every statement reads the snapshot of its
 * transaction and the schema stored in that snapshot; a writer takes the
 * lock before it pins anything it will write on. A statement that fails in
 * a transaction leaves the transaction as it was before the statement.
 */
export class SessionCore {
  private state: "open" | "closing" | "closed" = "open";

  private readonly queue = new TaskQueue();

  private readonly lifetime = new AbortController();

  private transaction: Transaction | null = null;

  private closing: Promise<void> | null = null;

  constructor(
    private readonly host: SessionHost,
    private readonly options: SessionOptions,
  ) {}

  /** True between BEGIN and COMMIT or ROLLBACK. */
  get inTransaction(): boolean {
    return this.transaction !== null;
  }

  /**
   * Runs one statement. Its parameters are checked and copied at once, so
   * what the caller does with them while the call waits changes nothing.
   */
  async query(
    sql: string,
    parameters: readonly Value[] | undefined,
    options: CallOptions,
  ): Promise<Result> {
    const values = parametersOf(parameters);
    return this.call(options, async (signal) => {
      const statement = onlyStatement(parseScript(sql), "query()");
      return this.runOne(new Step(sql, statement), values, signal);
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

  /** Parses and plans `sql`, so a statement that cannot run fails here. */
  async prepare(sql: string, options: CallOptions): Promise<Step> {
    return this.call(options, async (signal) => {
      const step = new Step(sql, onlyStatement(parseScript(sql), "prepare()"));
      if (step.kind !== "transaction") {
        await this.withSchema(signal, (schema) => step.plan(schema));
      }
      return step;
    });
  }

  /** Runs a prepared statement; its parameters are checked as query()'s are. */
  async runPrepared(
    step: Step,
    parameters: readonly Value[] | undefined,
    options: CallOptions,
  ): Promise<Result> {
    const values = parametersOf(parameters);
    return this.call(options, async (signal) =>
      this.runOne(step, values, signal),
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
      this.withSchema(signal, (schema) => describeSchema(schema)),
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

  /**
   * One call's turn: its task, then no transaction left where none may
   * stay — whether the task succeeded or failed, so no path through a task
   * can leave one behind.
   */
  private async inTurn<T>(
    task: (signal: AbortSignal) => Promise<T> | T,
    signal: AbortSignal,
  ): Promise<T> {
    let hasSucceeded = false;
    try {
      const value = await task(signal);
      hasSucceeded = true;
      return value;
    } finally {
      this.endLeftoverTransaction(hasSucceeded);
    }
  }

  /**
   * A session shared by unrelated callers keeps no transaction past a call:
   * one left open is rolled back. A call that otherwise succeeded then fails
   * with MISUSE; a failed one keeps its own error.
   */
  private endLeftoverTransaction(hasSucceeded: boolean): void {
    const transaction = this.transaction;
    const isLeftOver =
      transaction !== null && this.options.callScopedTransactions === true;
    if (!isLeftOver) return;
    this.transaction = null;
    transaction.rollback();
    if (!hasSucceeded) return;
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
        transaction.run(step, NO_PARAMETERS),
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
        this.runOne(step, NO_PARAMETERS, signal),
      );
    } catch (error) {
      this.endBegunSince(before);
      throw error;
    }
  }

  /**
   * A script that writes takes the lock before its first statement. Its
   * statements are planned first, against the latest schema, as far as they
   * can be — up to the first DDL, which may change what later ones bind
   * against — so one that cannot bind fails without waiting for the lock.
   */
  private async beginForScript(
    steps: readonly Step[],
    signal: AbortSignal,
  ): Promise<Transaction> {
    const isWriting = steps.some((step) => step.kind === "write");
    if (isWriting) {
      await this.withLatestSchema(signal, (schema) => {
        planAhead(steps, schema);
      });
    }
    const mode = isWriting ? "immediate" : "deferred";
    return Transaction.begin(this.host, mode, signal);
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
   * A statement in a transaction of its own. A write is planned against the
   * latest schema before it waits for the lock, so one that cannot bind
   * fails at once; the plan after the lock is the one that counts.
   */
  private async runAlone(
    step: Step,
    parameters: ParameterValues,
    signal: AbortSignal,
  ): Promise<Result> {
    const isWrite = step.kind === "write";
    if (isWrite) {
      await this.withLatestSchema(signal, (schema) => {
        checkParameters(step.plan(schema), parameters);
      });
    }
    const mode = isWrite ? "immediate" : "deferred";
    const transaction = await Transaction.begin(this.host, mode, signal);
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
   * after the statement has planned, so a statement that cannot bind never
   * waits. A failure leaves the transaction as it was before the statement.
   */
  private async runInTransaction(
    transaction: Transaction,
    step: Step,
    parameters: ParameterValues,
    signal: AbortSignal,
  ): Promise<Result> {
    if (step.kind === "write") {
      checkParameters(step.plan(transaction.schema), parameters);
      await transaction.makeWritable(signal);
    }
    return transaction.run(step, parameters);
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
    this.transaction = await Transaction.begin(this.host, mode, signal);
  }

  /** A commit that fails ends the transaction all the same. */
  private commitNow(): void {
    const transaction = this.transaction;
    if (transaction === null) return;
    this.transaction = null;
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
    read: (schema: SchemaModel) => T,
  ): Promise<T> {
    const transaction = this.transaction;
    if (transaction !== null) return read(transaction.schema);
    return this.withLatestSchema(signal, read);
  }

  /** Runs `read` on the latest committed schema, pinned for the moment. */
  private async withLatestSchema<T>(
    signal: AbortSignal,
    read: (schema: SchemaModel) => T,
  ): Promise<T> {
    const pages = await this.host.transactions.beginRead(signal);
    try {
      return read(this.host.schemas.load(pages).schema);
    } finally {
      pages.end();
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
  if (statement === undefined || rest.length > 0) {
    throw new MisuseError(
      `${what} takes exactly one statement; run scripts with execute()`,
    );
  }
  return statement;
}
