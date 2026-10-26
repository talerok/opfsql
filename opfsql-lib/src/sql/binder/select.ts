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
  type Clause,
} from "./clause.js";
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
import { itemName, selectItems, type SelectItem } from "./select-list.js";

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
}

export function bindSelect(
  node: SelectNode,
  scope: Scope,
  orderBy: readonly OrderByNode[],
): BoundQuery {
  const source = bindSource(node, scope);
  const items = selectItems(node.selectList, scope);
  const aggregation = isAggregating(node, orderBy)
    ? startAggregation(node, items, scope)
    : null;
  const bindAbove = aboveFrom(scope, aggregation);
  const outputs = items.map((item) => bindOutput(item, bindAbove));
  const having =
    node.having === null ? null : bindHaving(node.having, scope, aggregation);
  const ordering = orderColumns(orderBy, outputs, {
    allowsExtras: !node.isDistinct,
    bind: bindAbove,
  });
  const grouped = aggregate(source, aggregation, having);
  return finish(grouped, outputs, ordering, node.isDistinct, scope);
}

/** FROM, and WHERE as a filter over it. */
function bindSource(node: SelectNode, scope: Scope): Plan {
  const from = bindFrom(node.from, scope);
  if (node.where === null) return from;
  const clause = clauseOf(scope, refuse("WHERE"));
  const condition = bindCondition(node.where, clause, "WHERE");
  return { kind: "filter", child: from, conditions: conjunctsOf(condition) };
}

function startAggregation(
  node: SelectNode,
  items: readonly SelectItem[],
  scope: Scope,
): Aggregation {
  const groups = bindGroups(node.groupBy, items, scope);
  const tables = new Set(scope.relations.map((item) => item.tableIndex));
  const grouping = new Grouping(groups, scope.context.newIndex(), tables);
  const collector = new AggregateCollector(scope.context.newIndex());
  return { collector, grouping };
}

/** How the select list, HAVING and ORDER BY bind: above the aggregate, if any. */
function aboveFrom(
  scope: Scope,
  aggregation: Aggregation | null,
): (expression: ParsedExpression) => Expression {
  if (aggregation === null) {
    const clause = clauseOf(scope, refuse("this query"));
    return (expression) => clause.bind(expression);
  }
  const { collector, grouping } = aggregation;
  const clause: Clause = clauseOf(scope, { kind: "collect", collector });
  return (expression) => grouping.rewrite(clause.bind(expression));
}

function bindOutput(
  item: SelectItem,
  bindAbove: (expression: ParsedExpression) => Expression,
): Output {
  if (item.kind === "column") {
    return { expression: item.column, name: item.column.name, alias: null };
  }
  const expression = bindAbove(item.expression);
  const name = itemName(item, expression);
  return { expression, name, alias: item.expression.alias };
}

function bindHaving(
  having: ParsedExpression,
  scope: Scope,
  aggregation: Aggregation | null,
): Expression {
  const bindAbove = aboveFrom(scope, aggregation);
  const clause: Clause = {
    ...clauseOf(scope, refuse("HAVING")),
    bind: bindAbove,
  };
  return bindCondition(having, clause, "HAVING");
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
    aggregates: collector.aggregates,
  };
  if (having === null) return node;
  return { kind: "filter", child: node, conditions: conjunctsOf(having) };
}

/** The project, DISTINCT, ORDER BY, and the project that drops sort-only columns. */
function finish(
  plan: Plan,
  outputs: readonly Output[],
  ordering: Ordering,
  isDistinct: boolean,
  scope: Scope,
): BoundQuery {
  const expressions = [
    ...outputs.map((output) => output.expression),
    ...ordering.extras,
  ];
  const tableIndex = scope.context.newIndex();
  const projected: Plan = {
    kind: "project",
    tableIndex,
    child: plan,
    expressions,
  };
  const distinct: Plan = isDistinct
    ? { kind: "distinct", child: projected }
    : projected;
  const columns = outputs.map((output) => ({
    name: output.name,
    type: output.expression.type,
  }));
  const ordered = order(
    distinct,
    ordering.keys,
    columnsOf(tableIndex, expressions),
  );
  if (ordering.extras.length === 0) return { plan: ordered, columns };
  const trimmed: Plan = {
    kind: "project",
    tableIndex: scope.context.newIndex(),
    child: ordered,
    expressions: columnsOf(tableIndex, expressions).slice(0, outputs.length),
  };
  return { plan: trimmed, columns };
}

/** An order node over `plan` sorting by its columns at the keys' positions. */
export function order(
  plan: Plan,
  keys: readonly KeyColumn[],
  columns: readonly ColumnReference[],
): Plan {
  if (keys.length === 0) return plan;
  const orderKeys = keys.flatMap((key): OrderKey<Plan>[] => {
    const column = columns[key.position];
    if (column === undefined) return [];
    return [{ expression: column, direction: key.direction, nulls: key.nulls }];
  });
  return { kind: "order", child: plan, keys: orderKeys };
}

/** References to a project's columns. */
function columnsOf(
  tableIndex: number,
  expressions: readonly Expression[],
): ColumnReference[] {
  return expressions.map((expression, columnIndex) => ({
    kind: "column",
    binding: { tableIndex, columnIndex },
    depth: 0,
    name:
      expression.kind === "column"
        ? expression.name
        : `column${String(columnIndex + 1)}`,
    type: expression.type,
  }));
}
