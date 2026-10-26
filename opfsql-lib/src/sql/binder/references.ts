import { nameKey } from "../../schema/index.js";
import {
  childExpressions,
  compoundMembers,
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
// itself, and where. A walk over the syntax tree, before anything binds. A
// name that a nearer WITH defines is that query, not a table outside: it is
// not a mention.
// ---------------------------------------------------------------------------

/**
 * True if `statement`, its subqueries and WITH queries included, names
 * `table` where no WITH of its own defines that name.
 */
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

/** The names `statement` reads from outside itself. */
function* tablesIn(statement: SelectStatement): Generator<string> {
  const defined = new Set<string>();
  const isRecursive = statement.with?.isRecursive ?? false;
  const ctes = statement.with?.ctes ?? [];
  for (const cte of ctes) {
    const key = nameKey(cte.name);
    // A WITH query sees the ones before it, and itself in WITH RECURSIVE.
    const seen = isRecursive ? new Set([...defined, key]) : defined;
    yield* undefinedIn(tablesIn(cte.query), seen);
    defined.add(key);
  }
  yield* undefinedIn(tablesInBody(statement.body), defined);
  for (const order of statement.orderBy) {
    yield* undefinedIn(tablesInExpression(order.expression), defined);
  }
}

/** The names that are not among `defined`. */
function* undefinedIn(
  names: Iterable<string>,
  defined: ReadonlySet<string>,
): Generator<string> {
  for (const name of names) {
    if (!defined.has(nameKey(name))) yield name;
  }
}

function* tablesInBody(body: SelectNode | SetOperationNode): Generator<string> {
  const { first, links } = compoundMembers(body);
  const selects = [first, ...links.map((link) => link.select)];
  for (const select of selects) yield* tablesInSelect(select);
}

function* tablesInSelect(select: SelectNode): Generator<string> {
  if (select.from !== null) yield* tablesInFrom(select.from);
  for (const expression of expressionsOf(select)) {
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
