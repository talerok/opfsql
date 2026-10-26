import type { ParsedExpression } from "../parser/index.js";
import type { logical } from "../plan/index.js";
import { refuse, type Clause } from "./clause.js";
import { clauseOf } from "./expression.js";
import { hasColumn } from "./names.js";
import { aliasPosition, bareName, literalPosition } from "./select-list.js";
import type { Scope } from "./scope.js";
import { aliasOf, type SelectItem } from "./select-list.js";

// ---------------------------------------------------------------------------
// GROUP BY, as SQLite and PostgreSQL read it: an integer literal is a 1-based
// position in the select list, and a bare name that no column of the FROM
// has but an item's alias does is that item. Anything else is an
// expression over the FROM. Aggregates are not allowed in any of them.
// ---------------------------------------------------------------------------

/** The groups, and which select items they name. */
export interface Groups {
  readonly expressions: readonly logical.Expression[];
  /** For a select item a group names by position or alias: that group. */
  readonly ofItem: ReadonlyMap<number, number>;
}

/**
 * The GROUP BY expressions. A select item named by position or alias is
 * bound once, for its group: the select list reads it from the group, so a
 * subquery in it runs once per group.
 */
export function bindGroups(
  groupBy: readonly ParsedExpression[],
  items: readonly SelectItem[],
  scope: Scope,
): Groups {
  const clause = clauseOf(scope, refuse("GROUP BY"));
  const expressions: logical.Expression[] = [];
  const ofItem = new Map<number, number>();
  for (const group of groupBy) {
    const item = namedItem(group, items, scope);
    const earlier = item === null ? undefined : ofItem.get(item);
    if (item !== null && earlier === undefined) {
      ofItem.set(item, expressions.length);
    }
    const reused = earlier === undefined ? undefined : expressions[earlier];
    expressions.push(reused ?? groupExpression(group, item, items, clause));
  }
  return { expressions, ofItem };
}

function groupExpression(
  group: ParsedExpression,
  item: number | null,
  items: readonly SelectItem[],
  clause: Clause,
): logical.Expression {
  const named = item === null ? undefined : items[item];
  if (named === undefined) return clause.bind(group);
  return named.kind === "column" ? named.column : clause.bind(named.expression);
}

/** The position of the select item a GROUP BY expression names, by position or alias. */
function namedItem(
  group: ParsedExpression,
  items: readonly SelectItem[],
  scope: Scope,
): number | null {
  const position = literalPosition(group, items.length, "GROUP BY");
  if (position !== null) return position;
  const name = bareName(group);
  if (name === null || hasColumn(scope, name)) return null;
  const aliases = items.map((item) => aliasOf(item));
  return aliasPosition("GROUP BY", name, aliases);
}
