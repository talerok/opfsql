import { InternalError } from "../../errors.js";
import type {
  OrderByNode,
  ParsedExpression,
  SelectNode,
} from "../parser/index.js";
import type { ColumnReference, logical, OrderKey } from "../plan/index.js";
import {
  AggregateCollector,
  bindCondition,
  conjunctsOf,
  refuse,
} from "./clause.js";
import { nameOf, readColumns } from "./columns.js";
import { clauseOf } from "./expression.js";
import { bindFrom } from "./from-clause.js";
import { bindGroups } from "./group-by.js";
import { Grouping, isAggregating } from "./grouping.js";
import {
  orderColumns,
  type KeyColumn,
  type Ordering,
  type Output,
} from "./order-by.js";
import type { BoundQuery, Scope } from "./scope.js";
import {
  aliasOf,
  itemName,
  selectItems,
  type SelectItem,
} from "./select-list.js";
import { checkCondition } from "./type-rules.js";

// ---------------------------------------------------------------------------
// One SELECT: FROM and WHERE, the aggregate node if it groups (grouping.ts),
// HAVING above it, the select list as a project, then DISTINCT and ORDER BY.
// ORDER BY sorts by the project's columns; a key the select list does not
// show is computed as an extra column and dropped by a project above.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;
type Plan = logical.Plan;

/** What binds above the FROM: the aggregate's collector and groups, if it groups. */
interface Aggregation {
  readonly collector: AggregateCollector;
  readonly grouping: Grouping;
  /** For a select item GROUP BY named by position or alias: its group. */
  readonly groupOfItem: ReadonlyMap<number, number>;
}

export function bindSelect(
  node: SelectNode,
  scope: Scope,
  orderBy: readonly OrderByNode[],
): BoundQuery {
  const source = bindSource(node, scope);
  const items = selectItems(node.selectList, scope);
  const aggregation = startAggregation(node, orderBy, items, scope);
  const above = aboveFrom(scope, aggregation);
  const outputs = items.map((item, position) =>
    bindOutput(item, position, above),
  );
  const having = node.having === null ? null : bindHaving(node.having, above);
  const ordering = orderColumns(orderBy, outputs, {
    allowsExtras: !node.isDistinct,
    bind: (expression) => above.bind(expression),
  });
  const grouped = aggregate(source, aggregation, having);
  return finish(
    grouped,
    { outputs, ordering, isDistinct: node.isDistinct },
    scope,
  );
}

/** FROM, and WHERE as a filter over it. */
function bindSource(node: SelectNode, scope: Scope): Plan {
  const from = bindFrom(node.from, scope);
  if (node.where === null) return from;
  const clause = clauseOf(scope, refuse("WHERE"));
  const condition = bindCondition(node.where, clause, "WHERE");
  return { kind: "filter", child: from, conditions: conjunctsOf(condition) };
}

/** The aggregation of a query that groups (grouping.ts); null if it does not. */
function startAggregation(
  node: SelectNode,
  orderBy: readonly OrderByNode[],
  items: readonly SelectItem[],
  scope: Scope,
): Aggregation | null {
  if (!isAggregating(node, orderBy)) return null;
  const groups = bindGroups(node.groupBy, items, scope);
  const tables = new Set(scope.relations.map((item) => item.tableIndex));
  const groupIndex = scope.context.newIndex();
  const grouping = new Grouping(groups.expressions, groupIndex, tables);
  const collector = new AggregateCollector(scope.context.newIndex());
  return { collector, grouping, groupOfItem: groups.ofItem };
}

/**
 * How the select list, HAVING and ORDER BY bind: over the FROM, then lifted
 * above the aggregate if the query groups (grouping.ts).
 */
interface Above {
  bind(expression: ParsedExpression): Expression;
  lift(expression: Expression): Expression;
  /** The group column of select item `item`, if GROUP BY named the item. */
  groupOf(item: number): Expression | null;
}

function aboveFrom(scope: Scope, aggregation: Aggregation | null): Above {
  if (aggregation === null) {
    const clause = clauseOf(scope, refuse("this query"));
    return {
      bind: (expression) => clause.bind(expression),
      lift: (expression) => expression,
      groupOf: () => null,
    };
  }
  const { collector, grouping, groupOfItem } = aggregation;
  const clause = clauseOf(scope, { kind: "collect", collector });
  return {
    bind: (expression) => grouping.rewrite(clause.bind(expression)),
    lift: (expression) => grouping.rewrite(expression),
    groupOf: (item) => {
      const group = groupOfItem.get(item);
      return group === undefined ? null : grouping.column(group);
    },
  };
}

function bindOutput(item: SelectItem, position: number, above: Above): Output {
  const group = above.groupOf(position);
  if (group !== null) {
    return {
      expression: group,
      name: itemName(item, group),
      alias: aliasOf(item),
    };
  }
  if (item.kind === "column") {
    const expression = above.lift(item.column);
    return { expression, name: item.column.name, alias: null };
  }
  const expression = above.bind(item.expression);
  const name = itemName(item, expression);
  return { expression, name, alias: item.expression.alias };
}

function bindHaving(having: ParsedExpression, above: Above): Expression {
  const condition = above.bind(having);
  checkCondition(condition.type, "HAVING");
  return condition;
}

/** The aggregate node over the source, and HAVING as a filter over it. */
function aggregate(
  source: Plan,
  aggregation: Aggregation | null,
  having: Expression | null,
): Plan {
  if (aggregation === null) return source;
  const { collector, grouping } = aggregation;
  const node: Plan = {
    kind: "aggregate",
    groupIndex: grouping.groupIndex,
    aggregateIndex: collector.aggregateIndex,
    child: source,
    groups: grouping.groups,
    aggregates: [...collector.aggregates],
  };
  if (having === null) return node;
  return { kind: "filter", child: node, conditions: conjunctsOf(having) };
}

/** What is computed above the source and the aggregate. */
interface Shape {
  readonly outputs: readonly Output[];
  readonly ordering: Ordering;
  readonly isDistinct: boolean;
}

/** The project, DISTINCT, ORDER BY, and the project that drops sort-only columns. */
function finish(plan: Plan, shape: Shape, scope: Scope): BoundQuery {
  const { outputs, ordering } = shape;
  const projected = projectOutputs(plan, shape, scope);
  const distinct = withDistinct(projected.plan, shape.isDistinct);
  const ordered = order(distinct, ordering.keys, projected.columns);
  const shown = projected.columns.slice(0, outputs.length);
  const columns = outputs.map((output) => ({
    name: output.name,
    type: output.expression.type,
  }));
  if (ordering.extras.length === 0) return { plan: ordered, columns };
  return { plan: keepOnly(ordered, shown, scope), columns };
}

/** The select list as a project, the columns only ORDER BY needs after it. */
function projectOutputs(
  plan: Plan,
  shape: Shape,
  scope: Scope,
): { readonly plan: Plan; readonly columns: readonly ColumnReference[] } {
  const expressions = [
    ...shape.outputs.map((output) => output.expression),
    ...shape.ordering.extras,
  ];
  const tableIndex = scope.context.newIndex();
  const projected: Plan = {
    kind: "project",
    tableIndex,
    child: plan,
    expressions,
  };
  const columns = readColumns(
    projected,
    expressions.map((expression, position) => ({
      name: nameOf(expression, `column${String(position + 1)}`),
      type: expression.type,
    })),
  );
  return { plan: projected, columns };
}

/** A project of `columns` alone, dropping what was only sorted by. */
function keepOnly(
  plan: Plan,
  columns: readonly ColumnReference[],
  scope: Scope,
): Plan {
  const tableIndex = scope.context.newIndex();
  return { kind: "project", tableIndex, child: plan, expressions: columns };
}

function withDistinct(plan: Plan, isDistinct: boolean): Plan {
  return isDistinct ? { kind: "distinct", child: plan } : plan;
}

/** An order node over `plan` sorting by its columns at the keys' positions. */
export function order(
  plan: Plan,
  keys: readonly KeyColumn[],
  columns: readonly ColumnReference[],
): Plan {
  if (keys.length === 0) return plan;
  const orderKeys = keys.map((key): OrderKey<Plan> => {
    const column = columns[key.position];
    if (column === undefined) {
      throw new InternalError(
        `an ORDER BY key sorts by column ${String(key.position + 1)} of ${String(columns.length)}`,
      );
    }
    return { expression: column, direction: key.direction, nulls: key.nulls };
  });
  return { kind: "order", child: plan, keys: orderKeys };
}
