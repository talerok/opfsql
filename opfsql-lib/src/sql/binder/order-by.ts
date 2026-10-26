import { SemanticError } from "../../errors.js";
import { nameKey } from "../../schema/index.js";
import {
  ExpressionClass,
  OrderByNullType,
  OrderType,
  type OrderByNode,
  type ParsedExpression,
} from "../parser/index.js";
import type { logical } from "../plan/index.js";
import { isSameExpression } from "./same-expression.js";

// ---------------------------------------------------------------------------
// ORDER BY. A key is an output column of the query: an integer literal is a
// 1-based position, a bare name that is an output's alias is that output,
// and any other expression is matched to an output it equals — or, unless
// the query is DISTINCT, computed as a column the output does not show.
// NULLs come first ascending and last descending, as in SQLite and as
// index keys order them.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;

/** A result column ORDER BY can name. */
export interface Output {
  readonly expression: Expression;
  readonly name: string;
  /** Its AS name, which a bare name in ORDER BY refers to. */
  readonly alias: string | null;
}

/** How a query lets ORDER BY name what it sorts by. */
export interface KeySource {
  /** False for SELECT DISTINCT: a key must be an output. */
  readonly allowsExtras: boolean;
  /** Binds an expression key as the select list's expressions are bound. */
  readonly bind: (expression: ParsedExpression) => Expression;
}

/** A sort key, as the position of the column it sorts by. */
export interface KeyColumn {
  readonly position: number;
  readonly direction: "ascending" | "descending";
  readonly nulls: "first" | "last";
}

/** Sort keys, and the columns past the outputs computed only to sort by. */
export interface Ordering {
  readonly keys: readonly KeyColumn[];
  readonly extras: readonly Expression[];
}

/** Each ORDER BY key as the column it sorts by: outputs, then extras. */
export function orderColumns(
  orderBy: readonly OrderByNode[],
  outputs: readonly Output[],
  source: KeySource,
): Ordering {
  const extras: Expression[] = [];
  const keys = orderBy.map((order) => ({
    position: keyPosition(order.expression, outputs, extras, source),
    ...keyOrder(order),
  }));
  return { keys, extras };
}

/** The direction and NULL placement of a key; NULLs are smallest by default. */
export function keyOrder(
  order: OrderByNode,
): Pick<KeyColumn, "direction" | "nulls"> {
  const isAscending = order.type === OrderType.ASCENDING;
  const direction = isAscending ? "ascending" : "descending";
  if (order.nullOrder === null) {
    return { direction, nulls: isAscending ? "first" : "last" };
  }
  const isFirst = order.nullOrder === OrderByNullType.NULLS_FIRST;
  return { direction, nulls: isFirst ? "first" : "last" };
}

/**
 * The 0-based column an integer literal names in `clause`, or null for
 * any other expression; out of range is an error.
 */
export function literalPosition(
  expression: ParsedExpression,
  count: number,
  clause: string,
): number | null {
  const isInteger =
    expression.expressionClass === ExpressionClass.CONSTANT &&
    expression.type === "INTEGER";
  if (!isInteger) return null;
  const position = expression.value;
  if (position < 1 || position > count) {
    throw new SemanticError(
      `${clause} position ${String(position)} is not between 1 and ${String(count)}`,
    );
  }
  return position - 1;
}

/** The single name `expression` is, if it is a bare name. */
export function bareName(expression: ParsedExpression): string | null {
  const isName =
    expression.expressionClass === ExpressionClass.COLUMN_REF &&
    expression.columnNames.length === 1 &&
    expression.path.length === 0;
  return isName ? (expression.columnNames[0] ?? null) : null;
}

function keyPosition(
  expression: ParsedExpression,
  outputs: readonly Output[],
  extras: Expression[],
  source: KeySource,
): number {
  const position =
    literalPosition(expression, outputs.length, "ORDER BY") ??
    outputAliased(expression, outputs);
  if (position !== null) return position;
  const key = source.bind(expression);
  const columns = [...outputs.map((output) => output.expression), ...extras];
  const found = columns.findIndex((column) => isSameExpression(column, key));
  if (found !== -1) return found;
  if (!source.allowsExtras) {
    throw new SemanticError(
      "for SELECT DISTINCT, ORDER BY expressions must appear in the select list",
    );
  }
  extras.push(key);
  return columns.length;
}

/** The output whose alias a bare name is, as ORDER BY reads it. */
function outputAliased(
  expression: ParsedExpression,
  outputs: readonly Output[],
): number | null {
  const name = bareName(expression);
  if (name === null) return null;
  const isSame = (a: number, b: number): boolean => {
    const left = outputs[a]?.expression;
    const right = outputs[b]?.expression;
    return (
      left !== undefined && right !== undefined && isSameExpression(left, right)
    );
  };
  const aliases = outputs.map((output) => output.alias);
  return aliasPosition("ORDER BY", name, aliases, isSame);
}

/**
 * The position of the item `name` is the alias of, in GROUP BY or ORDER BY;
 * null if none. Two items of that alias must be the same, else the name is
 * ambiguous.
 */
export function aliasPosition(
  clause: string,
  name: string,
  aliases: readonly (string | null)[],
  isSame: (a: number, b: number) => boolean,
): number | null {
  const key = nameKey(name);
  const positions = aliases
    .map((alias, position) =>
      alias !== null && nameKey(alias) === key ? position : -1,
    )
    .filter((position) => position !== -1);
  const [first] = positions;
  if (first === undefined) return null;
  if (positions.some((position) => !isSame(first, position))) {
    throw new SemanticError(`${clause} ${name} is ambiguous`);
  }
  return first;
}
