import { SemanticError } from "../../errors.js";
import {
  isReservedName,
  Schema,
  type IndexKey,
  type IndexShape,
  type Table,
} from "../../schema/index.js";
import {
  formatExpression,
  parseExpression,
  type CreateIndexStatement,
  type ParsedExpression,
} from "../parser/index.js";
import type { CreateIndex, logical } from "../plan/index.js";
import { refuse } from "./clause.js";
import { asCorruption } from "./corruption.js";
import { checkNewName } from "./create-table.js";
import { tableNamed } from "./dml.js";
import { clauseOf } from "./expression.js";
import { bindQuery } from "./query.js";
import { BindContext, relation, Scope } from "./scope.js";
import { storedType } from "./type-rules.js";
import { partsOf } from "./walk.js";

// ---------------------------------------------------------------------------
// CREATE INDEX. A key is a column of the table, or an expression over its
// columns, stored as canonical SQL text (formatExpression) with the type it
// binds to. An expression key must give the same value for a row every
// time: no subqueries, parameters, aggregates or volatile functions.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;

const SUBQUERIES: ReadonlySet<Expression["kind"]> = new Set<Expression["kind"]>(
  ["scalar-subquery", "exists", "quantified-comparison"],
);

export function bindCreateIndex(
  statement: CreateIndexStatement,
  context: BindContext,
): CreateIndex {
  const { schema } = context;
  checkNewName(statement.name, "index", schema, statement.ifNotExists);
  const table = tableNamed(statement.table, context);
  if (isReservedName(table.name)) {
    throw new SemanticError(`table ${table.name} may not be indexed`);
  }
  return {
    kind: "create-index",
    index: indexDefinition(statement, table, context),
    isIfNotExists: statement.ifNotExists,
  };
}

/** The index a CREATE INDEX describes over `table`; its name is not checked. */
export function indexDefinition(
  statement: CreateIndexStatement,
  table: Table,
  context: BindContext,
): IndexShape {
  const scope = tableScope(table, context.newIndex(), context);
  return {
    name: statement.name,
    tableName: table.name,
    keys: statement.expressions.map((expression) =>
      indexKey(expression, scope),
    ),
    isUnique: statement.isUnique,
    origin: "statement",
  };
}

/**
 * A stored expression key of `table`, bound as CREATE INDEX binds it, over
 * the table's columns bound to `tableIndex` (column index = position): what
 * the engine compiles to compute the key of a row. A key that does not
 * parse or bind is a CorruptError.
 */
export function bindIndexKey(
  sql: string,
  table: Table,
  tableIndex: number,
): Expression {
  return asCorruption(
    () => bindKeyText(sql, table, tableIndex),
    (problem) =>
      `the index key ${sql} of table ${table.name} does not bind: ${problem}`,
  );
}

/** An expression key's text bound over `table`; its errors are a statement's. */
export function bindKeyText(
  sql: string,
  table: Table,
  tableIndex: number,
): Expression {
  const context = new BindContext(Schema.empty, bindQuery);
  const scope = tableScope(table, tableIndex, context);
  return bindKeyExpression(parseExpression(sql), scope);
}

function tableScope(
  table: Table,
  tableIndex: number,
  context: BindContext,
): Scope {
  const scope = Scope.root(context);
  scope.add(relation(table.name, tableIndex, table.columns));
  return scope;
}

function indexKey(expression: ParsedExpression, scope: Scope): IndexKey {
  const bound = bindKeyExpression(expression, scope);
  if (bound.kind === "column") {
    return { kind: "column", column: bound.binding.columnIndex };
  }
  const sql = formatExpression(expression);
  return { kind: "expression", sql, type: storedType(bound.type) };
}

function bindKeyExpression(
  expression: ParsedExpression,
  scope: Scope,
): Expression {
  const bound = clauseOf(scope, refuse("an index")).bind(expression);
  checkDeterministic(bound);
  return bound;
}

/** A key's value depends on its row alone. */
function checkDeterministic(expression: Expression): void {
  if (SUBQUERIES.has(expression.kind)) {
    throw new SemanticError("subqueries are not allowed in an index");
  }
  if (expression.kind === "parameter") {
    throw new SemanticError("parameters are not allowed in an index");
  }
  const isVolatile =
    expression.kind === "function" && expression.function.isVolatile === true;
  if (isVolatile) {
    throw new SemanticError(
      `${formatCall(expression)} is not allowed in an index: it gives another value each call`,
    );
  }
  for (const part of partsOf(expression)) {
    if (part !== null) checkDeterministic(part);
  }
}

function formatCall(expression: Expression): string {
  return expression.kind === "function" ? `${expression.name}()` : "it";
}
