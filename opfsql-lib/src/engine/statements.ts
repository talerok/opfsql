import { InternalError, MisuseError, NotSupportedError } from "../errors.js";
import type { WriteStatement } from "../executor/index.js";
import type { Schema } from "../schema/index.js";
import { type BindableStatement, bindStatement } from "../sql/binder/index.js";
import { optimize } from "../sql/optimizer/index.js";
import {
  type BeginStatement,
  parse,
  type Statement,
  StatementType,
} from "../sql/parser/index.js";
import type {
  physical,
  Query,
  Statement as PlannedStatement,
} from "../sql/plan/index.js";
import { NO_PARAMETERS, type ParameterValues } from "../values/index.js";
import type { TransactionMode } from "./api/index.js";
import type { Ddl } from "./ddl.js";
import { explainStatement } from "./explain.js";
import { bindKey } from "./index-keys.js";

// ---------------------------------------------------------------------------
// Statements from text to plans: parsed once, then bound and optimized
// against a schema (docs/planning.md, The pipeline). A plan never changes,
// so a statement keeps it for as long as it runs against the same schema.
// ---------------------------------------------------------------------------

export function parseScript(sql: string): readonly Statement[] {
  return parse(sql);
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

/** A statement bound and optimized against one schema: what a transaction runs. */
export type Planned =
  | { readonly kind: "query"; readonly statement: Query<physical.Plan> }
  | { readonly kind: "write"; readonly statement: WriteStatement }
  | { readonly kind: "ddl"; readonly statement: Ddl }
  | {
      readonly kind: "explain";
      /** The plan as text: EXPLAIN's one row. */
      readonly text: string;
      /** The explained statement's: EXPLAIN takes its parameters, unread. */
      readonly parameterCount: number;
    };

/** EXPLAIN's one column. */
const EXPLAIN_COLUMNS = ["plan"];

/** Result column names; none for a statement that returns no rows. */
export function columnsOf(planned: Planned): readonly string[] {
  switch (planned.kind) {
    case "query": {
      return planned.statement.columns.map((column) => column.name);
    }
    case "explain": {
      return EXPLAIN_COLUMNS;
    }
    case "write":
    case "ddl": {
      return [];
    }
  }
}

/**
 * A statement to run, with its plan for the schema it last ran against:
 * planned again only when it runs against another one. A prepared
 * statement keeps one.
 */
export class Step {
  private last: { readonly schema: Schema; readonly planned: Planned } | null =
    null;

  constructor(
    readonly sql: string,
    readonly statement: Statement,
  ) {}

  get kind(): StatementKind {
    return statementKind(this.statement);
  }

  /** Result column names as of the last plan; empty for writes. */
  get columns(): readonly string[] {
    return this.last === null ? [] : columnsOf(this.last.planned);
  }

  plan(schema: Schema): Planned {
    if (this.last?.schema !== schema) {
      this.last = { schema, planned: planStatement(this.statement, schema) };
    }
    return this.last.planned;
  }
}

/** A statement must get exactly the parameters it uses, never NULL for one it lacks. */
export function checkParameters(
  planned: Planned,
  parameters: ParameterValues = NO_PARAMETERS,
): void {
  const expected = parameterCountOf(planned);
  if (parameters.length === expected) return;
  throw new MisuseError(
    `the statement takes ${String(expected)} parameters, but ${String(parameters.length)} were given`,
  );
}

function parameterCountOf(planned: Planned): number {
  switch (planned.kind) {
    case "query":
    case "write": {
      return planned.statement.parameterCount;
    }
    case "explain": {
      return planned.parameterCount;
    }
    case "ddl": {
      return 0;
    }
  }
}

function planStatement(statement: Statement, schema: Schema): Planned {
  if (statement.type === StatementType.TRANSACTION_STATEMENT) {
    throw new InternalError("BEGIN, COMMIT and ROLLBACK have no plan");
  }
  if (statement.type === StatementType.EXPLAIN_STATEMENT) {
    return explain(statement.statement, schema);
  }
  return plannedOf(optimized(statement, schema));
}

function optimized(
  statement: BindableStatement,
  schema: Schema,
): PlannedStatement<physical.Plan> {
  return optimize(bindStatement(statement, schema), schema, bindKey);
}

function explain(subject: Statement, schema: Schema): Planned {
  if (subject.type === StatementType.TRANSACTION_STATEMENT) {
    throw new NotSupportedError("EXPLAIN of BEGIN, COMMIT or ROLLBACK");
  }
  if (subject.type === StatementType.EXPLAIN_STATEMENT) {
    throw new NotSupportedError("EXPLAIN of EXPLAIN");
  }
  const statement = optimized(subject, schema);
  const parameterCount = parameterCountOf(plannedOf(statement));
  return { kind: "explain", text: explainStatement(statement), parameterCount };
}

function plannedOf(statement: PlannedStatement<physical.Plan>): Planned {
  switch (statement.kind) {
    case "query": {
      return { kind: "query", statement };
    }
    case "insert":
    case "update":
    case "delete": {
      return { kind: "write", statement };
    }
    case "create-table":
    case "create-index":
    case "drop":
    case "alter-table": {
      return { kind: "ddl", statement };
    }
  }
}
