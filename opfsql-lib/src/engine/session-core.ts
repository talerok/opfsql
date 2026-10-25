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
import {
  beginMode,
  freshStep,
  parseScript,
  PreparedPlan,
  statementKind,
  type Step,
} from "./statements.js";
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

/** How a failed step's error is reported: located in a script, as is otherwise. */
type Report = (error: unknown, index: number) => unknown;

const asIs: Report = (error) => error;
const located: Report = (error, index) =>
  locateStatement(toOpfsqlError(error), index);

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
    this.context = { store: host.createStore(), host, owner: this };
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
    sql: string,
    task: (signal: AbortSignal) => Promise<T> | T,
  ): Promise<T> {
    if (this.state !== "open") throw new MisuseError("the session is closed");
    const linked = linkSignals([options.signal, this.lifetime.signal]);
    try {
      return await this.queue.run(async () => {
        const value = await task(linked.signal);
        this.endCallScope();
        return value;
      }, linked.signal);
    } catch (error) {
      throw toOpfsqlError(error, sql);
    } finally {
      linked.dispose();
    }
  }

  /** A session shared by unrelated callers keeps no transaction past a call. */
  private endCallScope(): void {
    const transaction = this.transaction;
    if (transaction === null || this.options.callScopedTransactions !== true) {
      return;
    }
    this.transaction = null;
    transaction.rollback();
    throw new MisuseError(
      "a call on a shared connection must end the transaction it begins; it was rolled back",
    );
  }

  /**
   * Outside a transaction, statements that do not control transactions run
   * as one; otherwise each runs as if sent alone, and a failure rolls back
   * a transaction begun by these steps, so no call leaves one behind.
   */
  private async runSteps(
    steps: readonly Step[],
    parameters: ParameterValues,
    signal: AbortSignal,
    report: Report,
  ): Promise<Result[]> {
    const hasControl = steps.some(
      (step) => statementKind(step.statement) === "transaction",
    );
    if (!hasControl && this.transaction === null) {
      return this.runAtomically(steps, parameters, signal, report);
    }
    const before = this.transaction;
    const results: Result[] = [];
    for (const [index, step] of steps.entries()) {
      try {
        results.push(await this.runOne(step, parameters, signal));
      } catch (error) {
        this.endBegunSince(before);
        throw report(error, index);
      }
    }
    return results;
  }

  private async runAtomically(
    steps: readonly Step[],
    parameters: ParameterValues,
    signal: AbortSignal,
    report: Report,
  ): Promise<Result[]> {
    if (steps.length === 0) return [];
    const isWriting = steps.some(
      (step) => statementKind(step.statement) === "write",
    );
    const mode = isWriting ? "immediate" : "deferred";
    const transaction = await Transaction.begin(this.context, mode, signal);
    const results: Result[] = [];
    for (const [index, step] of steps.entries()) {
      try {
        results.push(transaction.run(step, parameters));
      } catch (error) {
        transaction.rollback();
        throw report(error, index);
      }
    }
    transaction.commit();
    return results;
  }

  private async runOne(
    step: Step,
    parameters: ParameterValues,
    signal: AbortSignal,
  ): Promise<Result> {
    const statement = step.statement;
    if (statement.type === StatementType.TRANSACTION_STATEMENT) {
      await this.control(statement, signal);
      return emptyResult;
    }
    const transaction = this.transaction;
    if (transaction === null) {
      const results = await this.runAtomically(
        [step],
        parameters,
        signal,
        asIs,
      );
      return results.at(0) ?? emptyResult;
    }
    if (transaction.isAborted) throw new TransactionAbortedError();
    try {
      if (statementKind(statement) === "write") {
        await transaction.makeWritable(signal);
      }
      return transaction.run(step, parameters);
    } catch (error) {
      transaction.abort();
      throw error;
    }
  }

  private async control(
    statement: TransactionStatement,
    signal: AbortSignal,
  ): Promise<void> {
    switch (statement.transaction_type) {
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
  private withSchema<T>(read: (schema: VersionedSchema) => T): T {
    const transaction = this.transaction;
    if (transaction?.isAborted === true) throw new TransactionAbortedError();
    if (transaction !== null) return read(transaction.schema);
    const { store } = this.context;
    const position = this.host.pin(store);
    try {
      return read(this.host.schemas.load(store, position));
    } finally {
      store.endSnapshot();
    }
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
    return this.call(options, sql, async (signal) => {
      const statement = onlyStatement(parseScript(sql), "query()");
      const results = await this.runSteps(
        [freshStep(statement)],
        parameters,
        signal,
        asIs,
      );
      return results.at(0) ?? emptyResult;
    });
  }

  async execute(script: string, options: CallOptions): Promise<Result[]> {
    return this.call(options, script, async (signal) => {
      const steps = parseScript(script).map((statement) =>
        freshStep(statement),
      );
      return this.runSteps(steps, undefined, signal, located);
    });
  }

  /** Parses and binds `sql`, so a statement that cannot run fails here. */
  async prepare(sql: string, options: CallOptions): Promise<PreparedPlan> {
    return this.call(options, sql, () => {
      const statement = onlyStatement(parseScript(sql), "prepare()");
      const plan = new PreparedPlan(sql, statement);
      if (statementKind(statement) !== "transaction") {
        this.withSchema((schema) => plan.bind(schema.catalog));
      }
      return plan;
    });
  }

  async runPrepared(
    plan: PreparedPlan,
    parameters: ParameterValues,
    options: CallOptions,
  ): Promise<Result> {
    return this.call(options, plan.sql, async (signal) => {
      const results = await this.runSteps([plan], parameters, signal, asIs);
      return results.at(0) ?? emptyResult;
    });
  }

  async begin(options: TransactionOptions): Promise<void> {
    if (this.options.callScopedTransactions === true) {
      throw new MisuseError(
        "this session cannot keep a transaction open between calls; use a connection of its own",
      );
    }
    await this.call(options, "", async (signal) => {
      await this.beginNow(options.mode ?? "deferred", signal);
    });
  }

  async commit(options: CallOptions): Promise<void> {
    await this.call(options, "", () => {
      this.commitNow();
    });
  }

  async rollback(options: CallOptions): Promise<void> {
    await this.call(options, "", () => {
      this.rollbackNow();
    });
  }

  async schema(options: CallOptions): Promise<Schema> {
    return this.call(options, "", () =>
      this.withSchema((schema) => describeSchema(schema.catalog)),
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
}

function onlyStatement(statements: Statement[], what: string): Statement {
  const [statement, ...rest] = statements;
  if (statements.length === 0 || rest.length > 0) {
    throw new MisuseError(
      `${what} takes exactly one statement; run scripts with execute()`,
    );
  }
  return statement;
}
