import { MisuseError } from "../errors.js";
import { execute } from "../executor/execute/index.js";
import { outputColumnNames } from "../executor/execute/result.js";
import type { CatalogChange } from "../executor/types.js";
import { Binder } from "../sql/binder/index.js";
import {
  BoundExpressionClass,
  type LogicalOperator,
} from "../sql/binder/types.js";
import { optimize } from "../sql/optimizer/index.js";
import {
  Parser,
  StatementType,
  type Statement,
  type TransactionStatement,
} from "../sql/parser/index.js";
import type { Catalog } from "../store/catalog.js";
import type { SyncIIndexManager } from "../store/types.js";
import type { Value } from "../values/index.js";
import { Result, type TransactionMode } from "./api/index.js";
import { formatPlan } from "./explain.js";
import type { RecordingTableManager } from "./rows.js";

export function parseScript(sql: string): Statement[] {
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

/** BEGIN's mode: deferred unless it says IMMEDIATE. */
export function beginMode(statement: TransactionStatement): TransactionMode {
  return statement.mode === "IMMEDIATE" ? "immediate" : "deferred";
}

/**
 * A statement bound against one schema: what a prepared statement keeps.
 * The plan is never optimized in place — optimizer passes still rewrite the
 * trees they get, so each run optimizes a copy. Once plans are immutable,
 * the optimized plan can be kept here too.
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

export function bindStatement(
  statement: Statement,
  catalog: Catalog,
): BoundStatement {
  if (statement.type === StatementType.EXPLAIN_STATEMENT) {
    const plan = new Binder(catalog).bindStatement(statement.statement);
    const parameterCount = countParameters(plan);
    return {
      catalog,
      plan,
      columns: ["plan"],
      parameterCount,
      isExplain: true,
    };
  }
  const plan = new Binder(catalog).bindStatement(statement);
  const columns =
    statement.type === StatementType.SELECT_STATEMENT
      ? outputColumnNames(plan)
      : [];
  const parameterCount = countParameters(plan);
  return { catalog, plan, columns, parameterCount, isExplain: false };
}

/** The highest parameter number a plan refers to ($3 → 3), 0 for none. */
function countParameters(plan: LogicalOperator): number {
  let count = 0;
  const seen = new Set<object>();
  const visit = (node: unknown): void => {
    if (typeof node !== "object" || node === null || seen.has(node)) return;
    seen.add(node);
    if (isParameter(node)) count = Math.max(count, node.index + 1);
    for (const value of Object.values(node)) visit(value);
  };
  visit(plan);
  return count;
}

function isParameter(node: object): node is { index: number } {
  return (
    (node as { expressionClass?: unknown }).expressionClass ===
    BoundExpressionClass.BOUND_PARAMETER
  );
}

/** A statement to run: it binds itself against the schema it runs on. */
export interface Step {
  readonly statement: Statement;
  bind(catalog: Catalog): BoundStatement;
}

export function freshStep(statement: Statement): Step {
  return { statement, bind: (catalog) => bindStatement(statement, catalog) };
}

/** A prepared statement's insides: bound once, again when the schema changes. */
export class PreparedPlan implements Step {
  private bound: BoundStatement | null = null;

  constructor(
    readonly sql: string,
    readonly statement: Statement,
  ) {}

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
  if (parameters.length !== bound.parameterCount) {
    throw new MisuseError(
      `the statement takes ${String(bound.parameterCount)} parameters, but ${String(parameters.length)} were given`,
    );
  }
  const plan = optimize(structuredClone(bound.plan), target.catalog);
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
