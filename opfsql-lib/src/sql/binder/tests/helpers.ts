// Test helpers: a schema built from CREATE statements, statements bound
// against it, and plans printed, so that a test reads as one statement and
// the tree it binds to.

import { expect } from "vitest";
import {
  NotSupportedError,
  OpfsqlError,
  SemanticError,
} from "../../../errors.js";
import { Schema } from "../../../schema/index.js";
import { parse, StatementType, type Statement } from "../../parser/index.js";
import {
  formatPlan,
  type logical,
  type Statement as BoundStatement,
} from "../../plan/index.js";
import { bindStatement, type BindableStatement } from "../index.js";
import { applyStatement } from "./catalog.js";

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
  for (const sql of statements) {
    schema = applyStatement(schema, bindStatement(only(sql), schema));
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
export function query(sql: string, schema: Schema = shop): BoundOf<"query"> {
  return boundAs(sql, "query", schema);
}

type Bound = BoundStatement<logical.Plan>;

/** The bound statement of one kind. */
export type BoundOf<Kind extends Bound["kind"]> = Extract<
  Bound,
  { kind: Kind }
>;

/** `sql` bound, which must be a statement of `kind`. */
export function boundAs<Kind extends Bound["kind"]>(
  sql: string,
  kind: Kind,
  schema: Schema = shop,
): BoundOf<Kind> {
  const bound = bind(sql, schema);
  if (!isOfKind(bound, kind)) {
    throw new TypeError(`expected ${kind} from ${sql}, got ${bound.kind}`);
  }
  return bound;
}

function isOfKind<Kind extends Bound["kind"]>(
  bound: Bound,
  kind: Kind,
): bound is BoundOf<Kind> {
  return bound.kind === kind;
}

/** The subqueries of a query's plan, in print order, and which are correlated. */
export function subqueries(sql: string, schema: Schema = shop): string[] {
  return [
    ...plan(sql, schema).matchAll(/subquery\(\d+(?:, correlated)?\)(?=:)/g),
  ].map((match) => match[0]);
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
