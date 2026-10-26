import { nameKey } from "../../schema/index.js";
import {
  childExpressions,
  ExpressionClass,
  TableReferenceType,
  type ParsedExpression,
  type SelectNode,
  type SelectStatement,
  type SetOperationNode,
  type TableReference,
} from "../parser/index.js";

// ---------------------------------------------------------------------------
// The tables a query names, as written: whether a WITH query refers to
// itself, and where. A walk over the syntax tree, before anything binds.
// ---------------------------------------------------------------------------

/** True if `statement`, its subqueries and WITH queries included, names `table`. */
export function mentions(statement: SelectStatement, table: string): boolean {
  const key = nameKey(table);
  for (const name of tablesIn(statement)) {
    if (nameKey(name) === key) return true;
  }
  return false;
}

/** True if a subquery in `expression` names `table`. */
export function mentionsInExpression(
  expression: ParsedExpression,
  table: string,
): boolean {
  const key = nameKey(table);
  for (const name of tablesInExpression(expression)) {
    if (nameKey(name) === key) return true;
  }
  return false;
}

/** The expressions of one SELECT: its list, WHERE, GROUP BY, HAVING and ON conditions. */
export function* expressionsOf(node: SelectNode): Generator<ParsedExpression> {
  yield* node.selectList;
  if (node.where !== null) yield node.where;
  yield* node.groupBy;
  if (node.having !== null) yield node.having;
  if (node.from !== null) yield* joinConditions(node.from);
}

function* tablesIn(statement: SelectStatement): Generator<string> {
  for (const cte of statement.with?.ctes ?? []) yield* tablesIn(cte.query);
  yield* tablesInBody(statement.body);
  for (const order of statement.orderBy) {
    yield* tablesInExpression(order.expression);
  }
}

function* tablesInBody(body: SelectNode | SetOperationNode): Generator<string> {
  if (body.type === "SET_OPERATION_NODE") {
    yield* tablesInBody(body.left);
    yield* tablesInBody(body.right);
    return;
  }
  if (body.from !== null) yield* tablesInFrom(body.from);
  for (const expression of expressionsOf(body)) {
    yield* tablesInExpression(expression);
  }
}

function* tablesInFrom(from: TableReference): Generator<string> {
  switch (from.type) {
    case TableReferenceType.BASE_TABLE: {
      yield from.table;
      return;
    }
    case TableReferenceType.SUBQUERY: {
      yield* tablesIn(from.subquery);
      return;
    }
    case TableReferenceType.JOIN: {
      yield* tablesInFrom(from.left);
      yield* tablesInFrom(from.right);
    }
  }
}

function* tablesInExpression(expression: ParsedExpression): Generator<string> {
  if (expression.expressionClass === ExpressionClass.SUBQUERY) {
    yield* tablesIn(expression.subquery);
  }
  for (const child of childExpressions(expression)) {
    yield* tablesInExpression(child);
  }
}

function* joinConditions(from: TableReference): Generator<ParsedExpression> {
  if (from.type !== TableReferenceType.JOIN) return;
  yield* joinConditions(from.left);
  yield* joinConditions(from.right);
  if (from.constraint?.type === "ON") yield from.constraint.condition;
}
