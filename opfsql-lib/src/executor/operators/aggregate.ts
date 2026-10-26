import {
  compileAggregate,
  type CompiledAggregate,
  type CompiledExpression,
  compileExpression,
  type Tuple,
} from "../../expressions/index.js";
import type { Accumulator } from "../../functions/index.js";
import { physical } from "../../sql/plan/index.js";
import { rowKey, type Value } from "../../values/index.js";
import { type Context, scopeOf } from "../context.js";
import { drain, type Operator, WalkOperator } from "../operator.js";

/**
 * Groups keyed by rowKey — so two group values fall in one group exactly
 * when ORDER BY calls them equal, NULLs together — and the aggregates of
 * each. Without groups there is one group, even over no rows.
 */
interface Group {
  readonly values: readonly Value[];
  readonly accumulators: readonly Accumulator[];
}

/** What compiling the node gives: its group keys and its aggregates. */
interface Grouping {
  readonly keys: readonly CompiledExpression[];
  readonly aggregates: readonly CompiledAggregate[];
}

/** Its child's rows in groups: each group's values, then its aggregates. */
export function hashAggregate(
  plan: physical.HashAggregate,
  child: Operator,
  context: Context,
): Operator {
  const scope = scopeOf(context, physical.layoutOf(plan.child));
  const grouping: Grouping = {
    keys: plan.groups.map((group) => compileExpression(group, scope)),
    aggregates: plan.aggregates.map((call) => compileAggregate(call, scope)),
  };
  return new WalkOperator(() => groupRows(child, grouping).values(), [child]);
}

function groupRows(child: Operator, grouping: Grouping): Tuple[] {
  const groups = new Map<string, Group>();
  const isOneGroup = grouping.keys.length === 0;
  // Without GROUP BY there is one group, even over no rows.
  if (isOneGroup) groupOf(groups, [], grouping);
  for (const row of drain(child)) {
    const values = grouping.keys.map((key) => key(row));
    addRow(groupOf(groups, values, grouping), row, grouping);
  }
  return [...groups.values()].map((group) => [
    ...group.values,
    ...group.accumulators.map((accumulator) => accumulator.result()),
  ]);
}

/** The group of rows whose group values are `values`, started if new. */
function groupOf(
  groups: Map<string, Group>,
  values: readonly Value[],
  { aggregates }: Grouping,
): Group {
  const key = rowKey(values);
  const found = groups.get(key);
  if (found !== undefined) return found;
  const group = {
    values,
    accumulators: aggregates.map((aggregate) => aggregate.start()),
  };
  groups.set(key, group);
  return group;
}

function addRow(group: Group, row: Tuple, { aggregates }: Grouping): void {
  for (const [position, aggregate] of aggregates.entries()) {
    group.accumulators[position]?.add(aggregate.argumentsOf(row));
  }
}
