import { InternalError } from "../../errors.js";
import { physical } from "../../sql/plan/index.js";
import type { ColumnReference } from "../../sql/plan/index.js";
import { parametersOf, type Value } from "../../values/index.js";
import {
  compileExpression,
  compilePredicate,
  createRun,
  type Layout,
  type QueryContext,
  type Scope,
  type SubqueryCompiler,
  type Tuple,
} from "../index.js";
import { column } from "./builders.js";

// Compiling and running expressions in tests, and a fake executor that runs
// VALUES, projections and filters — enough of one for subqueries.

/** The table most tests read: t0 (c0 INTEGER, c1 TEXT, c2 ANY, c3 BOOLEAN). */
export const c0 = column(0, 0, "INTEGER");
export const c1 = column(0, 1, "TEXT");
export const c2 = column(0, 2, "ANY");
export const c3 = column(0, 3, "BOOLEAN");

export const LAYOUT: Layout = [c0, c1, c2, c3].map(
  (reference) => reference.binding,
);

/** The row most tests read. */
export const ROW: Tuple = [10, "hello", null, true];

/** `ROW` with `reference`'s value replaced by `value`. */
export function rowWith(reference: ColumnReference, value: Value): Tuple {
  const position = reference.binding.columnIndex;
  return ROW.map((old, index) => (index === position ? value : old));
}

export interface Options {
  readonly row?: Tuple;
  readonly parameters?: readonly Value[];
  readonly layout?: Layout;
  readonly subqueries?: SubqueryCompiler;
}

/** A scope of a run with the options' parameters, for `LAYOUT` by default. */
export function scopeOf(options: Options = {}): Scope {
  return {
    run: createRun(parametersOf(options.parameters)),
    outerLayouts: [],
    subqueries: options.subqueries ?? fakeExecutor(),
    layout: options.layout ?? LAYOUT,
  };
}

/** `expression`'s value for the options' row, `ROW` by default. */
export function evaluate(
  expression: physical.Expression,
  options: Options = {},
): Value {
  return compileExpression(expression, scopeOf(options))(options.row ?? ROW);
}

/** `expression` compiled once, and its value for each of `rows`. */
export function evaluateEach(
  expression: physical.Expression,
  rows: readonly Tuple[],
  options: Options = {},
): Value[] {
  const compiled = compileExpression(expression, scopeOf(options));
  return rows.map((row) => compiled(row));
}

/** Whether `condition` holds, as a WHERE clause decides it. */
export function holds(
  condition: physical.Expression,
  options: Options = {},
): boolean {
  return compilePredicate(condition, scopeOf(options))(options.row ?? ROW);
}

/** A subquery compiler that records what it compiles and runs. */
export interface FakeExecutor extends SubqueryCompiler {
  /** The plans compiled, in order. */
  readonly compiled: readonly physical.Plan[];
  /** The limit each run asked for, in order: one entry per run. */
  readonly limits: readonly (number | undefined)[];
}

/** A compiled plan: all its rows, computed afresh on each call. */
type Producer = () => Tuple[];

export function fakeExecutor(): FakeExecutor {
  const compiled: physical.Plan[] = [];
  const limits: (number | undefined)[] = [];
  const executor: FakeExecutor = {
    compiled,
    limits,
    compile: (plan, level) => {
      compiled.push(plan);
      const produce = producerOf(plan, { ...level, subqueries: executor });
      return {
        rows: (limit) => {
          limits.push(limit);
          const rows = produce();
          return limit === undefined ? rows : rows.slice(0, limit);
        },
      };
    },
  };
  return executor;
}

function producerOf(plan: physical.Plan, context: QueryContext): Producer {
  if (plan.kind === "values") return valuesProducer(plan, context);
  if (plan.kind === "project") return projectProducer(plan, context);
  if (plan.kind === "filter") return filterProducer(plan, context);
  throw new InternalError(`the fake executor does not run ${plan.kind}`);
}

function valuesProducer(
  plan: physical.Values,
  context: QueryContext,
): Producer {
  const scope: Scope = { ...context, layout: [] };
  const rows = plan.rows.map((row) =>
    row.map((expression) => compileExpression(expression, scope)),
  );
  return () => rows.map((row) => row.map((value) => value([])));
}

function projectProducer(
  plan: physical.Project,
  context: QueryContext,
): Producer {
  const child = producerOf(plan.child, context);
  const scope: Scope = { ...context, layout: physical.layoutOf(plan.child) };
  const expressions = plan.expressions.map((expression) =>
    compileExpression(expression, scope),
  );
  return () =>
    child().map((row) => expressions.map((expression) => expression(row)));
}

function filterProducer(
  plan: physical.Filter,
  context: QueryContext,
): Producer {
  const child = producerOf(plan.child, context);
  const scope: Scope = { ...context, layout: physical.layoutOf(plan.child) };
  const conditions = plan.conditions.map((condition) =>
    compilePredicate(condition, scope),
  );
  return () =>
    child().filter((row) => conditions.every((condition) => condition(row)));
}
