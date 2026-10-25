import { MisuseError } from "../errors.js";
import { execute } from "../executor/execute/index.js";
import { outputColumnNames } from "../executor/execute/result.js";
import type { CatalogChange } from "../executor/types.js";
import { Binder } from "../sql/binder/index.js";
import type { LogicalOperator } from "../sql/binder/types.js";
import { optimize } from "../sql/optimizer/index.js";
import {
  Parser,
  type Statement,
  StatementType,
  type BeginStatement,
} from "../sql/parser/index.js";
import type { Catalog } from "../store/catalog.js";
import type { SyncIIndexManager } from "../store/types.js";
import type { Value } from "../values/index.js";
import { Result, type TransactionMode } from "./api/index.js";
import { formatPlan } from "./explain.js";
import type { RecordingTableManager } from "./rows.js";

export function parseScript(sql: string): readonly Statement[] {
  return new Parser().parse(sql);
}

/** What a statement needs from its transaction. */
export type StatementKind = "read" | "write" | "transaction";

export function statementKind(statement: Statement): StatementKind {
  switch (statement.type) {
    case StatementType.SELECT_STATEMENT:
    case StatementType.EXPLAIN_STATEMENT: {
      return "read";
    }
    case StatementType.INSERT_STATEMENT:
    case StatementType.UPDATE_STATEMENT:
    case StatementType.DELETE_STATEMENT:
    case StatementType.CREATE_TABLE_STATEMENT:
    case StatementType.CREATE_INDEX_STATEMENT:
    case StatementType.ALTER_TABLE_STATEMENT:
    case StatementType.DROP_STATEMENT: {
      return "write";
    }
    case StatementType.TRANSACTION_STATEMENT: {
      return "transaction";
    }
  }
}

/** DDL: what the statements after it in a script may bind against. */
export function changesSchema(statement: Statement): boolean {
  switch (statement.type) {
    case StatementType.CREATE_TABLE_STATEMENT:
    case StatementType.CREATE_INDEX_STATEMENT:
    case StatementType.ALTER_TABLE_STATEMENT:
    case StatementType.DROP_STATEMENT: {
      return true;
    }
    case StatementType.SELECT_STATEMENT:
    case StatementType.EXPLAIN_STATEMENT:
    case StatementType.INSERT_STATEMENT:
    case StatementType.UPDATE_STATEMENT:
    case StatementType.DELETE_STATEMENT:
    case StatementType.TRANSACTION_STATEMENT: {
      return false;
    }
  }
}

/** BEGIN's mode: deferred unless it says IMMEDIATE. */
export function beginMode(statement: BeginStatement): TransactionMode {
  return statement.mode === "IMMEDIATE" ? "immediate" : "deferred";
}

/**
 * A statement bound against one schema. The plan is never optimized in
 * place — optimizer passes still rewrite the trees they get, so each run
 * optimizes a copy. Once plans are immutable, the optimized plan can be
 * kept here too.
 */
export interface BoundStatement {
  /** The schema it was bound against; a run on another one binds again. */
  readonly catalog: Catalog;
  readonly plan: LogicalOperator;
  readonly columns: readonly string[];
  /** How many parameters it takes: the highest $n it uses. */
  readonly parameterCount: number;
  /** EXPLAIN: the plan is optimized and described, never run. */
  readonly isExplain: boolean;
}

function bindStatement(statement: Statement, catalog: Catalog): BoundStatement {
  const binder = new Binder(catalog);
  const isExplain = statement.type === StatementType.EXPLAIN_STATEMENT;
  const subject = isExplain ? statement.statement : statement;
  const plan = binder.bindStatement(subject);
  return {
    catalog,
    plan,
    columns: columnsOf(subject, plan, isExplain),
    parameterCount: binder.parameterCount,
    isExplain,
  };
}

function columnsOf(
  statement: Statement,
  plan: LogicalOperator,
  isExplain: boolean,
): readonly string[] {
  if (isExplain) return ["plan"];
  if (statement.type !== StatementType.SELECT_STATEMENT) return [];
  return outputColumnNames(plan);
}

/**
 * A statement to run, with the bind of its last run: bound again only when
 * it runs against another schema. A prepared statement keeps one.
 */
export class Step {
  private bound: BoundStatement | null = null;

  constructor(
    readonly sql: string,
    readonly statement: Statement,
  ) {}

  get kind(): StatementKind {
    return statementKind(this.statement);
  }

  /** Result column names as of the last bind; empty for writes. */
  get columns(): readonly string[] {
    return this.bound?.columns ?? [];
  }

  bind(catalog: Catalog): BoundStatement {
    if (this.bound?.catalog !== catalog) {
      this.bound = bindStatement(this.statement, catalog);
    }
    return this.bound;
  }
}

/** A statement must get exactly the parameters it uses, never NULL for one it lacks. */
export function checkParameters(
  bound: BoundStatement,
  parameters: readonly Value[] = [],
): void {
  if (parameters.length === bound.parameterCount) return;
  throw new MisuseError(
    `the statement takes ${String(bound.parameterCount)} parameters, but ${String(parameters.length)} were given`,
  );
}

/** Where a statement reads and writes. */
export interface Target {
  readonly catalog: Catalog;
  readonly rows: RecordingTableManager;
  readonly indexes: SyncIIndexManager;
}

export interface Outcome {
  readonly result: Result;
  /** DDL the statement made, for its transaction's schema. */
  readonly changes: readonly CatalogChange[];
}

export function runBound(
  bound: BoundStatement,
  target: Target,
  parameters: readonly Value[] = [],
): Outcome {
  checkParameters(bound, parameters);
  const plan = optimize(clonePlan(bound.plan), target.catalog);
  if (bound.isExplain) {
    const rows = [[formatPlan(plan)]];
    return { result: resultOf(bound.columns, rows), changes: [] };
  }
  const output = execute(
    plan,
    target.rows,
    target.catalog,
    target.indexes,
    parameters,
  );
  const result = new Result({
    columns: bound.columns,
    rows: output.rows,
    changes: output.rowsAffected,
    lastInsertRowid: target.rows.lastInsertRowid,
  });
  return { result, changes: output.catalogChanges };
}

function resultOf(
  columns: readonly string[],
  rows: readonly (readonly Value[])[],
): Result {
  return new Result({ columns, rows, changes: 0, lastInsertRowid: null });
}

/**
 * A copy of a bound plan for the optimizer, which still changes plans in
 * place: plain objects and arrays are copied; anything else — frozen
 * function definitions, BLOB constants — is shared. It goes once plans are
 * immutable (docs/planning.md).
 */
function clonePlan<T>(value: T): T {
  if (Array.isArray(value))
    return value.map((item: unknown) => clonePlan(item)) as T;
  if (!isCopied(value)) return value;
  const entries = Object.entries(value).map(([key, item]) => [
    key,
    clonePlan(item),
  ]);
  return Object.fromEntries(entries) as T;
}

function isCopied(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Object.isFrozen(value) &&
    Object.getPrototypeOf(value) === Object.prototype
  );
}
