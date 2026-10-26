import { nameKey } from "../../schema/index.js";
import type { ParsedExpression } from "../parser/index.js";
import type { logical } from "../plan/index.js";
import { refuse, type Clause } from "./clause.js";
import { clauseOf } from "./expression.js";
import { hasColumn } from "./names.js";
import { bareName, literalPosition } from "./order-by.js";
import type { Scope } from "./scope.js";
import { aliasOf, type SelectItem } from "./select-list.js";

// ---------------------------------------------------------------------------
// GROUP BY, as SQLite and PostgreSQL read it: an integer literal is a 1-based
// position in the select list, and a bare name that no column of the FROM
// has but an item's alias does is that item. Anything else is an
// expression over the FROM. Aggregates are not allowed in any of them.
// ---------------------------------------------------------------------------

export function bindGroups(
  groupBy: readonly ParsedExpression[],
  items: readonly SelectItem[],
  scope: Scope,
): logical.Expression[] {
  const clause = clauseOf(scope, refuse("GROUP BY"));
  return groupBy.map((group) => {
    const item = namedItem(group, items, scope);
    return item === null ? clause.bind(group) : itemExpression(item, clause);
  });
}

/** The select item a GROUP BY expression names, by position or alias. */
function namedItem(
  group: ParsedExpression,
  items: readonly SelectItem[],
  scope: Scope,
): SelectItem | null {
  const position = literalPosition(group, items.length, "GROUP BY");
  if (position !== null) return items[position] ?? null;
  const name = bareName(group);
  if (name === null || hasColumn(scope, name)) return null;
  const key = nameKey(name);
  const isNamed = (item: SelectItem): boolean => {
    const alias = aliasOf(item);
    return alias !== null && nameKey(alias) === key;
  };
  return items.find((item) => isNamed(item)) ?? null;
}

function itemExpression(item: SelectItem, clause: Clause): logical.Expression {
  return item.kind === "column" ? item.column : clause.bind(item.expression);
}
