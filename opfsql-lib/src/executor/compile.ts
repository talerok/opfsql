import { InternalError } from "../errors.js";
import { createRun, type SubqueryCompiler } from "../expressions/index.js";
import type { physical } from "../sql/plan/index.js";
import type { PageReader } from "../storage/index.js";
import type { Value } from "../values/index.js";
import { type Context, levelOf } from "./context.js";
import { drain, type Operator, take } from "./operator.js";
import { hashAggregate } from "./operators/aggregate.js";
import { cte, cteScan, CteTable, recursiveCte } from "./operators/ctes.js";
import { hashJoin, nestedLoopJoin } from "./operators/joins.js";
import { limit } from "./operators/limit.js";
import { filter, hashDistinct, project, values } from "./operators/rows.js";
import { setOperation } from "./operators/sets.js";
import { sort, topN } from "./operators/sort.js";
import { indexExtreme } from "./scans/index-extreme.js";
import { indexScan } from "./scans/index-scan.js";
import { indexUnion } from "./scans/index-union.js";
import { rowidLookup } from "./scans/rowids.js";
import { tableScan } from "./scans/table-scan.js";

// ---------------------------------------------------------------------------
// Compiling a physical plan into operators, once per run (docs/planning.md,
// Execution): each node's expressions are compiled for its input's layout,
// and each subquery in them is compiled here too, once, as a query level of
// its own.
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
type TwoSidedPlan = Extract<physical.Plan, { readonly left: physical.Plan }>;
type CtePlan = Extract<
  physical.Plan,
  { readonly kind: "cte" | "cte-scan" | "recursive-cte" }
>;

/** The context of a statement's own query level, for a run with `parameters`. */
export function statementContext(
  pages: PageReader,
  parameters: readonly Value[],
): Context {
  const ctes = new Map<number, CteTable>();
  const run = createRun(parameters, subqueryCompiler(pages, ctes));
  return { query: { run, outerLayouts: [] }, ctes, pages };
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
    case "cte":
    case "cte-scan":
    case "recursive-cte": {
      return compileCte(plan, context);
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
 * A CTE's definition sees the CTEs around it; its body, and a recursive
 * CTE's step, see it as well.
 */
function compileCte(plan: CtePlan, context: Context): Operator {
  switch (plan.kind) {
    case "cte": {
      const table = new CteTable(plan.name, levelOf(context));
      const definition = compilePlan(plan.definition, context);
      const body = compilePlan(
        plan.body,
        withCte(context, plan.cteIndex, table),
      );
      return cte(table, definition, body);
    }
    case "cte-scan": {
      const table = context.ctes.get(plan.cteIndex);
      if (table === undefined) {
        throw new InternalError(`no CTE ${String(plan.cteIndex)} in scope`);
      }
      table.noteScanAt(levelOf(context));
      return cteScan(table);
    }
    case "recursive-cte": {
      const workingSet = new CteTable(plan.name, levelOf(context));
      const anchor = compilePlan(plan.anchor, context);
      const stepContext = withCte(context, plan.cteIndex, workingSet);
      const step = compilePlan(plan.step, stepContext);
      return recursiveCte(anchor, step, workingSet, plan.isUnionAll);
    }
  }
}

/**
 * `context` with one more CTE in scope — for its plan nodes, and for the
 * subqueries in their expressions, which are compiled where they are written.
 */
function withCte(context: Context, cteIndex: number, table: CteTable): Context {
  const ctes = new Map(context.ctes).set(cteIndex, table);
  const subqueries = subqueryCompiler(context.pages, ctes);
  const run = { ...context.query.run, subqueries };
  return { ...context, ctes, query: { ...context.query, run } };
}

/**
 * Compiles each subquery once, as a query level inside the one it is
 * written in; each call of `rows` starts it over, and stops at `limit` rows.
 */
function subqueryCompiler(
  pages: PageReader,
  ctes: ReadonlyMap<number, CteTable>,
): SubqueryCompiler {
  return {
    compile: (plan, query) => {
      const root = compilePlan(plan, { query, ctes, pages });
      return {
        rows: (count) => {
          root.rescan();
          return count === undefined ? drain(root) : take(root, count);
        },
      };
    },
  };
}
