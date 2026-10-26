// Test helpers: a schema built from CREATE statements, statements bound
// against it, and plans printed, so that a test reads as one statement and
// the tree it binds to.

import { expect } from "vitest";
import {
  NotSupportedError,
  OpfsqlError,
  SemanticError,
} from "../../../errors.js";
import type { Index, Table } from "../../../schema/index.js";
import { Schema } from "../../../schema/index.js";
import { parse, StatementType, type Statement } from "../../parser/index.js";
import {
  formatPlan,
  type logical,
  type Statement as BoundStatement,
} from "../../plan/index.js";
import { bindStatement, type BindableStatement } from "../index.js";

/** Users, their orders and the order lines: the schema most tests bind against. */
export const SHOP = [
  "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, email TEXT UNIQUE, data JSON)",
  "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, status TEXT)",
  "CREATE TABLE items (order_id INTEGER, sku TEXT, qty INTEGER, PRIMARY KEY (order_id, sku))",
  "CREATE INDEX orders_user ON orders (user_id)",
  "CREATE UNIQUE INDEX users_lower_name ON users (lower(name))",
] as const;

/** A schema with the tables and indexes `statements` create, in order. */
export function schemaOf(...statements: readonly string[]): Schema {
  let schema = Schema.empty;
  let rootPage = 2;
  const nextPage = (): number => ++rootPage;
  for (const sql of statements) {
    const bound = bindStatement(only(sql), schema);
    const tables: Table[] = [...schema.tables];
    const indexes: Index[] = [...schema.indexes];
    if (bound.kind === "create-table") {
      tables.push({ ...bound.table, rootPage: nextPage() });
      for (const index of bound.constraints) {
        indexes.push({ ...index, rootPage: nextPage() });
      }
    } else if (bound.kind === "create-index") {
      indexes.push({ ...bound.index, rootPage: nextPage() });
    } else {
      throw new TypeError(`not a CREATE statement: ${sql}`);
    }
    schema = new Schema(schema.version + 1, tables, indexes);
  }
  return schema;
}

export const shop = schemaOf(...SHOP);

/** The one statement of `sql`, which the binder takes. */
export function only(sql: string): BindableStatement {
  const statements = parse(sql);
  expect(statements).toHaveLength(1);
  const [statement] = statements;
  if (statement === undefined || !isBindable(statement)) {
    throw new TypeError(`not a statement the binder takes: ${sql}`);
  }
  return statement;
}

function isBindable(statement: Statement): statement is BindableStatement {
  return (
    statement.type !== StatementType.TRANSACTION_STATEMENT &&
    statement.type !== StatementType.EXPLAIN_STATEMENT
  );
}

export function bind(
  sql: string,
  schema: Schema = shop,
): BoundStatement<logical.Plan> {
  return bindStatement(only(sql), schema);
}

/** The plan a query binds to, printed. */
export function plan(sql: string, schema: Schema = shop): string {
  return formatPlan(query(sql, schema).plan);
}

/** A query bound. */
export function query(
  sql: string,
  schema: Schema = shop,
): Extract<BoundStatement<logical.Plan>, { kind: "query" }> {
  const bound = bind(sql, schema);
  if (bound.kind !== "query") throw new TypeError(`not a query: ${sql}`);
  return bound;
}

/** Lines of a printed plan, written as an array so a test can indent them. */
export function lines(...printed: readonly string[]): string {
  return printed.join("\n");
}

/** The message of the SemanticError binding `sql` throws. */
export function semanticError(sql: string, schema: Schema = shop): string {
  const error = thrown(sql, schema);
  if (!(error instanceof SemanticError)) {
    throw new TypeError(
      `expected a SemanticError from ${sql}, got ${String(error)}`,
    );
  }
  return error.message;
}

/** The feature of the NotSupportedError binding `sql` throws. */
export function notSupported(sql: string, schema: Schema = shop): string {
  const error = thrown(sql, schema);
  if (!(error instanceof NotSupportedError)) {
    throw new TypeError(
      `expected a NotSupportedError from ${sql}, got ${String(error)}`,
    );
  }
  return error.feature;
}

export function thrown(sql: string, schema: Schema = shop): OpfsqlError {
  try {
    bind(sql, schema);
  } catch (error) {
    if (error instanceof OpfsqlError) return error;
    throw error;
  }
  throw new Error(`bound without an error: ${sql}`);
}
