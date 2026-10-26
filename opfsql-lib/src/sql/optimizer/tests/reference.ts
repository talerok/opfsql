import { InternalError } from "../../../errors.js";
import type { Accumulator } from "../../../functions/index.js";
import {
  compileAggregate,
  compileExpression,
  compilePredicate,
  createRun,
  type Layout,
  type Run,
  type Tuple,
} from "../../../expressions/index.js";
import {
  compareValues,
  firstSighting,
  rowKey,
  type Value,
} from "../../../values/index.js";
import { type logical, physical } from "../../plan/index.js";
import type { KeyBinder } from "../index.js";
import { naivePlan } from "./naive.js";
import {
  compiled,
  type Context,
  extremeOf,
  fixed,
  scanned,
  type Tables,
} from "./scans.js";

// A reference interpreter for physical plans over rows in memory, written
// from the contract (physical.ts, docs/planning.md) and nothing else: each
// operator the simplest way, every row read. Held against the naive plan of
// the same logical plan (naive.ts), it checks what the optimizer chose.

/** The rows `plan` gives over `tables` with `parameters`. */
export function runPlan(
  plan: physical.Plan,
  tables: Tables,
  parameters: readonly Value[],
  bindKey: KeyBinder,
): readonly Tuple[] {
  const holder: { context?: Context } = {};
  const run = createRun(parameters, {
    compile: (subplan, query) => ({
      rows: (limit) => {
        if (holder.context === undefined) throw new InternalError("no run yet");
        const context = { ...holder.context, outerLayouts: query.outerLayouts };
        return evaluate(subplan, context).slice(0, limit);
      },
    }),
  });
  holder.context = { tables, run, outerLayouts: [], ctes: new Map(), bindKey };
  return evaluate(plan, holder.context);
}

/** The rows of the optimizer's plan and of the naive one, each as a multiset. */
export function bothRuns(
  plan: logical.Plan,
  optimized: physical.Plan,
  data: { readonly tables: Tables; readonly parameters: readonly Value[] },
  bindKey: KeyBinder,
): { readonly expected: string[]; readonly actual: string[] } {
  const { tables, parameters } = data;
  const expected = runPlan(naivePlan(plan), tables, parameters, bindKey);
  const actual = runPlan(optimized, tables, parameters, bindKey);
  return { expected: multiset(expected), actual: multiset(actual) };
}

/** The rows of both plans, in the order each gives them. */
export function bothSequences(
  plan: logical.Plan,
  optimized: physical.Plan,
  data: { readonly tables: Tables; readonly parameters: readonly Value[] },
  bindKey: KeyBinder,
): { readonly expected: string[]; readonly actual: string[] } {
  const { tables, parameters } = data;
  const expected = runPlan(naivePlan(plan), tables, parameters, bindKey);
  const actual = runPlan(optimized, tables, parameters, bindKey);
  return { expected: sequence(expected), actual: sequence(actual) };
}

/** Rows as a list of keys, in order. */
function sequence(rows: readonly Tuple[]): string[] {
  return rows.map((row) => rowKey(row));
}

/** Rows as a sorted list of keys, for results whose order is free. */
export function multiset(rows: readonly Tuple[]): string[] {
  return rows.map((row) => rowKey(row)).toSorted((a, b) => a.localeCompare(b));
}

function evaluate(plan: physical.Plan, context: Context): readonly Tuple[] {
  switch (plan.kind) {
    case "table-scan":
    case "rowid-lookup":
    case "index-scan":
    case "index-union": {
      return scanned(plan, context);
    }
    case "index-extreme": {
      return [[extremeOf(plan, context)]];
    }
    case "hash-join":
    case "nested-loop-join":
    case "lookup-join": {
      return joined(plan, context);
    }
    case "filter":
    case "project":
    case "hash-aggregate":
    case "hash-distinct": {
      return transformed(plan, context);
    }
    case "sort":
    case "top-n":
    case "limit": {
      return ordered(plan, context);
    }
    case "set-operation":
    case "cte":
    case "cte-scan":
    case "recursive-cte":
    case "values": {
      return combined(plan, context);
    }
  }
}

function scopeOf(
  child: physical.Plan,
  context: Context,
): {
  readonly layout: Layout;
  readonly outerLayouts: readonly Layout[];
  readonly run: Run;
} {
  const { outerLayouts, run } = context;
  return { layout: physical.layoutOf(child), outerLayouts, run };
}

function transformed(
  plan:
    | physical.Filter
    | physical.Project
    | physical.HashAggregate
    | physical.HashDistinct,
  context: Context,
): readonly Tuple[] {
  switch (plan.kind) {
    case "filter": {
      const scope = scopeOf(plan.child, context);
      const tests = plan.conditions.map((condition) =>
        compilePredicate(condition, scope),
      );
      return evaluate(plan.child, context).filter((row) =>
        tests.every((test) => test(row)),
      );
    }
    case "project": {
      const layout = physical.layoutOf(plan.child);
      const values = plan.expressions.map((expression) =>
        compiled(expression, layout, context),
      );
      return evaluate(plan.child, context).map((row) =>
        values.map((value) => value(row)),
      );
    }
    case "hash-aggregate": {
      return aggregated(plan, context);
    }
    case "hash-distinct": {
      const isFirst = firstSighting();
      return evaluate(plan.child, context).filter((row) => isFirst(row));
    }
  }
}

interface Group {
  readonly key: readonly Value[];
  readonly accumulators: readonly Accumulator[];
}

function aggregated(
  plan: physical.HashAggregate,
  context: Context,
): readonly Tuple[] {
  const scope = scopeOf(plan.child, context);
  const groups = plan.groups.map((group) => compileExpression(group, scope));
  const calls = plan.aggregates.map((call) => compileAggregate(call, scope));
  const start = (key: readonly Value[]): Group => ({
    key,
    accumulators: calls.map((call) => call.start()),
  });
  const found = new Map<string, Group>();
  for (const row of evaluate(plan.child, context)) {
    const key = groups.map((group) => group(row));
    const group = found.get(rowKey(key)) ?? start(key);
    found.set(rowKey(key), group);
    for (const [position, call] of calls.entries()) {
      group.accumulators[position]?.add(call.argumentsOf(row));
    }
  }
  // Without groups, no rows are still one group.
  if (found.size === 0 && plan.groups.length === 0) found.set("", start([]));
  return [...found.values()].map((group) => [
    ...group.key,
    ...group.accumulators.map((accumulator) => accumulator.result()),
  ]);
}

type Join = physical.HashJoin | physical.NestedLoopJoin | physical.LookupJoin;

function joined(plan: Join, context: Context): readonly Tuple[] {
  const { outerLayouts, run } = context;
  const layout = [
    ...physical.layoutOf(plan.left),
    ...physical.layoutOf(plan.right),
  ];
  const conditions = conditionsOf(plan);
  const tests = conditions.map((condition) =>
    compilePredicate(condition, { layout, outerLayouts, run }),
  );
  const rightsOf = rightRowsOf(plan, context);
  const nulls = physical.layoutOf(plan.right).map((): Value => null);
  const rows: Tuple[] = [];
  for (const left of evaluate(plan.left, context)) {
    const pairs = rightsOf(left)
      .map((right) => [...left, ...right])
      .filter((pair) => tests.every((test) => test(pair)));
    rows.push(...pairs);
    if (pairs.length === 0 && plan.joinType === "left") {
      rows.push([...left, ...nulls]);
    }
  }
  return rows;
}

/**
 * The right rows a left row meets: every one, or for a lookup join those
 * its scan finds with seek values read from that left row.
 */
function rightRowsOf(
  plan: Join,
  context: Context,
): (left: Tuple) => readonly Tuple[] {
  if (plan.kind !== "lookup-join") {
    const rights = evaluate(plan.right, context);
    return () => rights;
  }
  const layout = physical.layoutOf(plan.left);
  return (row) => scanned(plan.right, { ...context, seek: { layout, row } });
}

/**
 * What a pair must pass: a hash join's keys as `=`, then its residual; a
 * lookup join's seek has found its pairs, which pass its residual.
 */
function conditionsOf(plan: Join): readonly physical.Expression[] {
  switch (plan.kind) {
    case "nested-loop-join": {
      return plan.conditions;
    }
    case "lookup-join": {
      return plan.residual;
    }
    case "hash-join": {
      return [...keyEqualities(plan), ...plan.residual];
    }
  }
}

/** A hash join's keys as the `=` they stand for: values as they are, NULL never matching. */
function keyEqualities(plan: physical.HashJoin): physical.Expression[] {
  return plan.leftKeys.map((left, position) => {
    const right = plan.rightKeys[position];
    if (right === undefined) throw new InternalError("a key without its pair");
    return {
      kind: "comparison",
      operator: "EQUAL",
      left,
      right,
      type: "BOOLEAN",
    };
  });
}

function ordered(
  plan: physical.Sort | physical.TopN | physical.Limit,
  context: Context,
): readonly Tuple[] {
  const rows = evaluate(plan.child, context);
  if (plan.kind === "limit") {
    return limited(rows, plan.limit, plan.offset, context);
  }
  const layout = physical.layoutOf(plan.child);
  const sorted = rows.toSorted(compareByKeys(plan.keys, layout, context.run));
  if (plan.kind === "sort") return sorted;
  return limited(sorted, plan.limit, plan.offset, context);
}

/** The order of rows by `keys`, NULLs placed as each key says. */
export function compareByKeys(
  keys: readonly physical.OrderKey[],
  layout: Layout,
  run: Run,
): (a: Tuple, b: Tuple) => number {
  const sides = keys.map((key) => ({
    key,
    value: compileExpression(key.expression, { layout, outerLayouts: [], run }),
  }));
  return (a, b) => {
    for (const { key, value } of sides) {
      const order = compareKey(value(a), value(b), key);
      if (order !== 0) return order;
    }
    return 0;
  };
}

function compareKey(a: Value, b: Value, key: physical.OrderKey): number {
  if (a === null || b === null) {
    if (a === b) return 0;
    const isFirst = (a === null) === (key.nulls === "first");
    return isFirst ? -1 : 1;
  }
  const order = compareValues(a, b);
  return key.direction === "ascending" ? order : -order;
}

/** LIMIT and OFFSET as docs/semantics.md takes them; the tests give integers. */
function limited(
  rows: readonly Tuple[],
  limit: physical.Expression | null,
  offset: physical.Expression | null,
  context: Context,
): readonly Tuple[] {
  const count = limit === null ? -1 : Number(fixed(limit, context));
  const skipped =
    offset === null ? 0 : Math.max(0, Number(fixed(offset, context)));
  return count < 0 ? rows.slice(skipped) : rows.slice(skipped, skipped + count);
}

function combined(
  plan:
    | physical.SetOperation
    | physical.Cte
    | physical.CteScan
    | physical.RecursiveCte
    | physical.Values,
  context: Context,
): readonly Tuple[] {
  switch (plan.kind) {
    case "set-operation": {
      return setOperation(plan, context);
    }
    case "cte": {
      context.ctes.set(plan.cteIndex, evaluate(plan.definition, context));
      return evaluate(plan.body, context);
    }
    case "cte-scan": {
      return context.ctes.get(plan.cteIndex) ?? [];
    }
    case "recursive-cte": {
      throw new InternalError(
        "the reference interpreter runs no recursive CTE",
      );
    }
    case "values": {
      return plan.rows.map((row) =>
        row.map((expression) => fixed(expression, context)),
      );
    }
  }
}

function setOperation(
  plan: physical.SetOperation,
  context: Context,
): readonly Tuple[] {
  const left = evaluate(plan.left, context);
  const right = evaluate(plan.right, context);
  const inRight = new Set(right.map((row) => rowKey(row)));
  const isFirst = firstSighting();
  switch (plan.operator) {
    case "UNION ALL": {
      return [...left, ...right];
    }
    case "UNION": {
      return [...left, ...right].filter((row) => isFirst(row));
    }
    case "INTERSECT": {
      return left.filter((row) => inRight.has(rowKey(row)) && isFirst(row));
    }
    case "EXCEPT": {
      return left.filter((row) => !inRight.has(rowKey(row)) && isFirst(row));
    }
  }
}
