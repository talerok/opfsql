import { InternalError } from "../errors.js";
import {
  type CompiledSubquery,
  createRun,
  type QueryContext,
  type Scope,
  type SubqueryCompiler,
  type Tuple,
} from "../expressions/index.js";
import { physical } from "../sql/plan/index.js";
import type { Tables } from "../table/index.js";
import type { ParameterValues } from "../values/index.js";
import { type Context, type CteScope, levelOf, scopeOf } from "./context.js";
import { drain, type Operator, take } from "./operator.js";
import { hashAggregate } from "./operators/aggregate.js";
import { cte, CteRows, cteScan, recursiveCte } from "./operators/ctes.js";
import { hashJoin, lookupJoin, nestedLoopJoin } from "./operators/joins.js";
import { limit } from "./operators/limit.js";
import { filter, hashDistinct, project } from "./operators/per-row.js";
import { setOperation } from "./operators/sets.js";
import { sort, topN } from "./operators/sort.js";
import { values } from "./operators/values.js";
import { indexExtreme } from "./scans/index-extreme.js";
import { indexScan, indexScanRows } from "./scans/index-scan.js";
import { indexUnion } from "./scans/index-union.js";
import { rowidLookup, rowidLookupRows } from "./scans/rowids.js";
import { tableScan } from "./scans/table-scan.js";

// ---------------------------------------------------------------------------
// Compiling a physical plan into operators, once per run (docs/planning.md,
// Execution): each node's expressions are compiled for its input's layout,
// and each subquery in them is compiled here too, once, as a query level of
// its own, with the CTEs in scope where it is written.
// ---------------------------------------------------------------------------

type ScanPlan = Extract<
  physical.Plan,
  {
    readonly kind:
      | "table-scan"
      | "rowid-lookup"
      | "index-scan"
      | "index-union"
      | "index-extreme";
  }
>;
type OneChildPlan = Extract<physical.Plan, { readonly child: physical.Plan }>;
type TwoSidedPlan = Extract<
  physical.Plan,
  { readonly kind: "hash-join" | "nested-loop-join" | "set-operation" }
>;

/**
 * The context of a statement's own query level, for a run with `parameters`
 * and the engine's `checkpoint`.
 */
export function statementContext(
  tables: Tables,
  parameters: ParameterValues,
  checkpoint?: () => void,
): Context {
  const ctes: CteScope = new Map();
  const run = createRun(parameters, subqueriesIn(ctes, tables), checkpoint);
  return contextIn({ run, outerLayouts: [] }, ctes, tables);
}

export function compilePlan(plan: physical.Plan, context: Context): Operator {
  switch (plan.kind) {
    case "table-scan":
    case "rowid-lookup":
    case "index-scan":
    case "index-union":
    case "index-extreme": {
      return compileScan(plan, context);
    }
    case "filter":
    case "project":
    case "hash-aggregate":
    case "sort":
    case "top-n":
    case "limit":
    case "hash-distinct": {
      return compileOverChild(plan, context);
    }
    case "hash-join":
    case "nested-loop-join":
    case "set-operation": {
      return compileTwoSided(plan, context);
    }
    case "lookup-join": {
      return compileLookupJoin(plan, context);
    }
    case "cte": {
      return compileCte(plan, context);
    }
    case "cte-scan": {
      return compileCteScan(plan, context);
    }
    case "recursive-cte": {
      return compileRecursiveCte(plan, context);
    }
    case "values": {
      return values(plan, context);
    }
  }
}

function compileScan(plan: ScanPlan, context: Context): Operator {
  switch (plan.kind) {
    case "table-scan": {
      return tableScan(plan, context);
    }
    case "rowid-lookup": {
      return rowidLookup(plan, context);
    }
    case "index-scan": {
      return indexScan(plan, context);
    }
    case "index-union": {
      return indexUnion(plan, context);
    }
    case "index-extreme": {
      return indexExtreme(plan, context);
    }
  }
}

function compileOverChild(plan: OneChildPlan, context: Context): Operator {
  const child = compilePlan(plan.child, context);
  switch (plan.kind) {
    case "filter": {
      return filter(plan, child, context);
    }
    case "project": {
      return project(plan, child, context);
    }
    case "hash-aggregate": {
      return hashAggregate(plan, child, context);
    }
    case "sort": {
      return sort(plan, child, context);
    }
    case "top-n": {
      return topN(plan, child, context);
    }
    case "limit": {
      return limit(plan, child, context);
    }
    case "hash-distinct": {
      return hashDistinct(child);
    }
  }
}

function compileTwoSided(plan: TwoSidedPlan, context: Context): Operator {
  const left = compilePlan(plan.left, context);
  const right = compilePlan(plan.right, context);
  switch (plan.kind) {
    case "hash-join": {
      return hashJoin(plan, left, right, context);
    }
    case "nested-loop-join": {
      return nestedLoopJoin(plan, left, right, context);
    }
    case "set-operation": {
      return setOperation(plan, left, right);
    }
  }
}

/**
 * The left side, and the right scan compiled to start for each left row:
 * its seek values in the scope of the left side's rows, its residual over
 * its own.
 */
function compileLookupJoin(
  plan: physical.LookupJoin,
  context: Context,
): Operator {
  const left = compilePlan(plan.left, context);
  const seeks = scopeOf(context, physical.layoutOf(plan.left));
  const lookup = compileLookup(plan.right, context, seeks);
  return lookupJoin(plan, left, lookup, context);
}

function compileLookup(
  plan: physical.LookupJoin["right"],
  context: Context,
  seeks: Scope,
): (row: Tuple) => Generator<Tuple> {
  switch (plan.kind) {
    case "rowid-lookup": {
      return rowidLookupRows(plan, context, seeks);
    }
    case "index-scan": {
      return indexScanRows(plan, context, seeks);
    }
  }
}

/** The definition sees the CTEs around it; the body sees this one too. */
function compileCte(plan: physical.Cte, context: Context): Operator {
  const rows = new CteRows(plan.name);
  const definition = compilePlan(plan.definition, context);
  const body = compilePlan(plan.body, withCte(context, plan.cteIndex, rows));
  const at = { level: levelOf(context), run: context.query.run };
  return cte(rows, definition, body, at);
}

function compileCteScan(plan: physical.CteScan, context: Context): Operator {
  const rows = context.ctes.get(plan.cteIndex);
  if (rows === undefined) {
    throw new InternalError(`no CTE ${String(plan.cteIndex)} in scope`);
  }
  return cteScan(rows);
}

/** The anchor sees the CTEs around it; the step sees its working set too. */
function compileRecursiveCte(
  plan: physical.RecursiveCte,
  context: Context,
): Operator {
  const workingSet = new CteRows(plan.name);
  const anchor = compilePlan(plan.anchor, context);
  const step = compilePlan(
    plan.step,
    withCte(context, plan.cteIndex, workingSet),
  );
  const { isUnionAll } = plan;
  const { run } = context.query;
  return recursiveCte(anchor, step, { workingSet, isUnionAll, run });
}

/** `context` with one more CTE in scope. */
function withCte(context: Context, cteIndex: number, rows: CteRows): Context {
  const ctes = new Map(context.ctes).set(cteIndex, rows);
  return contextIn(context.query, ctes, context.tables);
}

/**
 * A query level with `ctes` in scope, whose run compiles the subqueries
 * written in it with the same CTEs in scope: the one place a context is
 * made, so the two cannot differ.
 */
function contextIn(
  query: QueryContext,
  ctes: CteScope,
  tables: Tables,
): Context {
  const run = { ...query.run, subqueries: subqueriesIn(ctes, tables) };
  return { query: { ...query, run }, ctes, tables };
}

/** Compiles the subqueries written where `ctes` are in scope. */
function subqueriesIn(ctes: CteScope, tables: Tables): SubqueryCompiler {
  return {
    compile: (plan, query) =>
      compiledSubquery(plan, contextIn(query, ctes, tables)),
  };
}

/**
 * A subquery compiled once, as a query level inside the one it is written
 * in: each call of `rows` starts it over and takes at most `count` rows.
 */
function compiledSubquery(
  plan: physical.Plan,
  context: Context,
): CompiledSubquery {
  const root = compilePlan(plan, context);
  return {
    rows: (count) => {
      root.rescan();
      return count === undefined ? drain(root) : take(root, count);
    },
  };
}
