import { SemanticError } from "../../errors.js";
import {
  OrderByNullType,
  type OrderByNode,
  type ParsedExpression,
} from "../parser/index.js";
import type { logical } from "../plan/index.js";
import { isSameExpression } from "./same-expression.js";
import { aliasPosition, bareName, literalPosition } from "./select-list.js";

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
  const keys: KeyColumn[] = [];
  for (const order of orderBy) {
    const target = keyTarget(order.expression, outputs, extras, source);
    if (target.kind === "extra") extras.push(target.expression);
    const position = positionOf(target, outputs.length + extras.length - 1);
    keys.push({ position, ...keyOrder(order) });
  }
  return { keys, extras };
}

/** The direction and NULL placement of a key; NULLs are smallest by default. */
export function keyOrder(
  order: OrderByNode,
): Pick<KeyColumn, "direction" | "nulls"> {
  const { direction } = order;
  const isAscending = direction === "ascending";
  if (order.nullOrder === null) {
    return { direction, nulls: isAscending ? "first" : "last" };
  }
  const isFirst = order.nullOrder === OrderByNullType.NULLS_FIRST;
  return { direction, nulls: isFirst ? "first" : "last" };
}

/** What a key sorts by: a column already there, or an expression to add. */
type KeyTarget =
  | { readonly kind: "column"; readonly position: number }
  | { readonly kind: "extra"; readonly expression: Expression };

/** A target's column: its own, or `added`, where the extra it adds went. */
function positionOf(target: KeyTarget, added: number): number {
  return target.kind === "column" ? target.position : added;
}

function keyTarget(
  expression: ParsedExpression,
  outputs: readonly Output[],
  extras: readonly Expression[],
  source: KeySource,
): KeyTarget {
  const named =
    literalPosition(expression, outputs.length, "ORDER BY") ??
    outputAliased(expression, outputs);
  if (named !== null) return { kind: "column", position: named };
  const key = source.bind(expression);
  const columns = [...outputs.map((output) => output.expression), ...extras];
  const position = columns.findIndex((column) => isSameExpression(column, key));
  if (position !== -1) return { kind: "column", position };
  if (!source.allowsExtras) {
    throw new SemanticError(
      "for SELECT DISTINCT, ORDER BY expressions must appear in the select list",
    );
  }
  return { kind: "extra", expression: key };
}

/** The output whose alias a bare name is, as ORDER BY reads it. */
function outputAliased(
  expression: ParsedExpression,
  outputs: readonly Output[],
): number | null {
  const name = bareName(expression);
  if (name === null) return null;
  const aliases = outputs.map((output) => output.alias);
  return aliasPosition("ORDER BY", name, aliases);
}
