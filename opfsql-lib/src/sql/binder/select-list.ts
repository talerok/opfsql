import {
  ExpressionClass,
  formatExpression,
  type ParsedExpression,
} from "../parser/index.js";
import type { ColumnReference, logical } from "../plan/index.js";
import { starColumns } from "./names.js";
import type { Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// The select list, its stars expanded: an item is an expression as written,
// or a column `*` stands for. GROUP BY and ORDER BY name items by position
// and alias, so the expansion comes first.
// ---------------------------------------------------------------------------

export type SelectItem =
  | { readonly kind: "expression"; readonly expression: ParsedExpression }
  | { readonly kind: "column"; readonly column: ColumnReference };

export function selectItems(
  list: readonly ParsedExpression[],
  scope: Scope,
): SelectItem[] {
  return list.flatMap((expression): SelectItem[] => {
    if (expression.expressionClass !== ExpressionClass.STAR) {
      return [{ kind: "expression", expression }];
    }
    return starColumns(expression, scope).map((column) => ({
      kind: "column",
      column,
    }));
  });
}

/** The item's AS name, if it has one. */
export function aliasOf(item: SelectItem): string | null {
  return item.kind === "expression" ? item.expression.alias : null;
}

/**
 * The name of the result column an item makes: its alias, the name of the
 * column it reads, or the expression as SQL prints it (`count(*)`, `a + 1`).
 */
export function itemName(item: SelectItem, bound: logical.Expression): string {
  if (item.kind === "column") return item.column.name;
  const { expression } = item;
  if (expression.alias !== null) return expression.alias;
  const isColumn =
    expression.expressionClass === ExpressionClass.COLUMN_REF &&
    bound.kind === "column";
  return isColumn ? bound.name : formatExpression(expression);
}
