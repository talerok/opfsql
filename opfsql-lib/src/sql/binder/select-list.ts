import { SemanticError } from "../../errors.js";
import { nameKey } from "../../schema/index.js";
import {
  ExpressionClass,
  formatExpression,
  type ParsedExpression,
  type SelectListItem,
} from "../parser/index.js";
import type { ColumnReference, logical } from "../plan/index.js";
import { starColumns } from "./names.js";
import { oneOrNone } from "./one.js";
import type { Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// The select list, its stars expanded: an item is an expression as written,
// or a column `*` stands for. GROUP BY and ORDER BY name items by position
// and alias, so the expansion comes first.
// ---------------------------------------------------------------------------

export type SelectItem =
  | {
      readonly kind: "expression";
      readonly expression: ParsedExpression;
      readonly alias: string | null;
    }
  | { readonly kind: "column"; readonly column: ColumnReference };

export function selectItems(
  list: readonly SelectListItem[],
  scope: Scope,
): SelectItem[] {
  return list.flatMap(({ expression, alias }): SelectItem[] => {
    if (expression.expressionClass !== ExpressionClass.STAR) {
      return [{ kind: "expression", expression, alias }];
    }
    return starColumns(expression, scope).map((column) => ({
      kind: "column",
      column,
    }));
  });
}

/** The item's AS name, if it has one. */
export function aliasOf(item: SelectItem): string | null {
  return item.kind === "expression" ? item.alias : null;
}

/**
 * The name of the result column an item makes: its alias, the name of the
 * column it reads, or the expression as SQL prints it (`count(*)`, `a + 1`).
 */
export function itemName(item: SelectItem, bound: logical.Expression): string {
  if (item.kind === "column") return item.column.name;
  const { expression, alias } = item;
  if (alias !== null) return alias;
  const isColumn =
    expression.expressionClass === ExpressionClass.COLUMN_REF &&
    bound.kind === "column";
  return isColumn ? bound.name : formatExpression(expression);
}

/**
 * The 0-based column an integer literal names in `place`, or null for
 * any other expression; out of range is an error.
 */
export function literalPosition(
  expression: ParsedExpression,
  count: number,
  place: string,
): number | null {
  const isInteger =
    expression.expressionClass === ExpressionClass.CONSTANT &&
    expression.type === "INTEGER";
  if (!isInteger) return null;
  const position = expression.value;
  if (position < 1 || position > count) {
    throw new SemanticError(
      `${place} position ${String(position)} is not between 1 and ${String(count)}`,
    );
  }
  return position - 1;
}

/** The single name `expression` is, if it is a bare name. */
export function bareName(expression: ParsedExpression): string | null {
  const isName =
    expression.expressionClass === ExpressionClass.COLUMN_REF &&
    expression.names.length === 1 &&
    expression.path.length === 0;
  return isName ? (expression.names[0] ?? null) : null;
}

/**
 * The position of the one item whose alias is `name`, in GROUP BY or ORDER
 * BY; null if no item has it. An alias names one item: two items with it
 * make the name ambiguous, whatever they compute.
 */
export function aliasPosition(
  place: string,
  name: string,
  aliases: readonly (string | null)[],
): number | null {
  const key = nameKey(name);
  const positions = aliases
    .map((alias, position) =>
      alias !== null && nameKey(alias) === key ? position : -1,
    )
    .filter((position) => position !== -1);
  return oneOrNone(
    positions,
    () => new SemanticError(`${place} ${name} is ambiguous`),
  );
}
