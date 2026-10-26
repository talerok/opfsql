import { type ColumnBinding, physical } from "../plan/index.js";
import { columnsOf, holdsCorrelatedSubquery } from "./expression-tree.js";
import { seekValuesOf } from "./lookup.js";

// ---------------------------------------------------------------------------
// Pass 4, column pruning (docs/planning.md, Physical plans): a scan reads
// only the columns some node above it uses, and the rowid only when one
// does; the table layer then decodes only those. The binder lists every
// column of a table read; this pass, over the finished physical plan, walks
// it from the top with the bindings wanted of each node's output and keeps
// a scan's column only if it is wanted or its own residual reads it.
//
// What each node wants of its input:
//   filter, sort, top-n, limit   what is wanted of it, and what it reads;
//   joins                        the same, a lookup join's seek values
//                                reading its left side too;
//   project, hash-aggregate      what it computes from: its output is its own;
//   hash-distinct                every column: it compares whole rows;
//   set operations, CTEs         every column of each side and definition,
//                                which are read by position.
//
// An expression holding a correlated subquery wants every column of the
// row it is evaluated on: the subquery's plan reads that row by binding,
// from outside, where columnsOf cannot see.
// ---------------------------------------------------------------------------

type Plan = physical.Plan;
type Expression = physical.Expression;

/** The bindings wanted, as `tableIndex:columnIndex`. */
type Wanted = ReadonlySet<string>;

/** `plan` reading only what its output's `wanted` bindings need. */
export function pruneColumns(
  plan: Plan,
  wanted: readonly ColumnBinding[],
): Plan {
  return pruned(plan, new Set(wanted.map((binding) => keyOf(binding))));
}

/** `plan` with every column of its output wanted. */
export function pruneBelow(plan: Plan): Plan {
  return pruneColumns(plan, physical.layoutOf(plan));
}

function keyOf({ tableIndex, columnIndex }: ColumnBinding): string {
  return `${String(tableIndex)}:${String(columnIndex)}`;
}

/** `wanted`, and the columns of `layout` that `expressions` read. */
function withReads(
  wanted: Wanted,
  expressions: readonly Expression[],
  layout: readonly ColumnBinding[],
): Wanted {
  const all = new Set(wanted);
  for (const expression of expressions) {
    const read = holdsCorrelatedSubquery(expression)
      ? layout
      : [...columnsOf(expression)];
    for (const binding of read) all.add(keyOf(binding));
  }
  return all;
}

function pruned(plan: Plan, wanted: Wanted): Plan {
  switch (plan.kind) {
    case "table-scan":
    case "rowid-lookup":
    case "index-scan":
    case "index-union": {
      return prunedScan(plan, wanted);
    }
    case "hash-join":
    case "nested-loop-join":
    case "lookup-join": {
      return prunedJoin(plan, wanted);
    }
    case "filter":
    case "sort":
    case "top-n":
    case "limit": {
      return prunedPassing(plan, wanted);
    }
    case "project":
    case "hash-aggregate":
    case "hash-distinct": {
      return prunedComputing(plan);
    }
    case "set-operation":
    case "cte":
    case "recursive-cte": {
      return prunedCombining(plan, wanted);
    }
    case "index-extreme":
    case "cte-scan":
    case "values": {
      return plan;
    }
  }
}

/** A scan's columns that are wanted or that its residual reads, in order. */
function prunedScan<S extends physical.Scan>(scan: S, wanted: Wanted): S {
  const needed = withReads(wanted, scan.residual, physical.layoutOf(scan));
  const columns = scan.columns.filter((columnIndex) =>
    needed.has(keyOf({ tableIndex: scan.tableIndex, columnIndex })),
  );
  return { ...scan, columns };
}

type Join = Extract<
  Plan,
  { readonly kind: "hash-join" | "nested-loop-join" | "lookup-join" }
>;

function prunedJoin(plan: Join, wanted: Wanted): Plan {
  const layout = physical.layoutOf(plan);
  switch (plan.kind) {
    case "hash-join": {
      const reads = [...plan.leftKeys, ...plan.rightKeys, ...plan.residual];
      const needed = withReads(wanted, reads, layout);
      const left = pruned(plan.left, needed);
      return { ...plan, left, right: pruned(plan.right, needed) };
    }
    case "nested-loop-join": {
      const needed = withReads(wanted, plan.conditions, layout);
      const left = pruned(plan.left, needed);
      return { ...plan, left, right: pruned(plan.right, needed) };
    }
    case "lookup-join": {
      const needed = withReads(wanted, plan.residual, layout);
      const leftLayout = physical.layoutOf(plan.left);
      const seeks = withReads(needed, seekValuesOf(plan.right), leftLayout);
      const left = pruned(plan.left, seeks);
      return { ...plan, left, right: prunedScan(plan.right, needed) };
    }
  }
}

type Passing = Extract<
  Plan,
  { readonly kind: "filter" | "sort" | "top-n" | "limit" }
>;

/** A node whose rows are its child's: it wants what is wanted of it, and what it reads. */
function prunedPassing(plan: Passing, wanted: Wanted): Plan {
  const layout = physical.layoutOf(plan.child);
  const needed = withReads(wanted, readsOf(plan), layout);
  return { ...plan, child: pruned(plan.child, needed) };
}

function readsOf(plan: Passing): Expression[] {
  switch (plan.kind) {
    case "filter": {
      return [...plan.conditions];
    }
    case "sort": {
      return plan.keys.map((key) => key.expression);
    }
    case "top-n": {
      const keys = plan.keys.map((key) => key.expression);
      return [...keys, plan.limit, ...optional(plan.offset)];
    }
    case "limit": {
      return [...optional(plan.limit), ...optional(plan.offset)];
    }
  }
}

function optional(expression: Expression | null): Expression[] {
  return expression === null ? [] : [expression];
}

type Computing = Extract<
  Plan,
  { readonly kind: "project" | "hash-aggregate" | "hash-distinct" }
>;

/** A node whose output is its own: it wants what it computes from. */
function prunedComputing(plan: Computing): Plan {
  const layout = physical.layoutOf(plan.child);
  const none: Wanted = new Set();
  switch (plan.kind) {
    case "project": {
      const needed = withReads(none, plan.expressions, layout);
      return { ...plan, child: pruned(plan.child, needed) };
    }
    case "hash-aggregate": {
      const reads = [
        ...plan.groups,
        ...plan.aggregates.flatMap((call) => call.arguments),
      ];
      const needed = withReads(none, reads, layout);
      return { ...plan, child: pruned(plan.child, needed) };
    }
    case "hash-distinct": {
      return { ...plan, child: pruneBelow(plan.child) };
    }
  }
}

type Combining = Extract<
  Plan,
  { readonly kind: "set-operation" | "cte" | "recursive-cte" }
>;

/** Nodes over plans read by position: each of those keeps every column. */
function prunedCombining(plan: Combining, wanted: Wanted): Plan {
  switch (plan.kind) {
    case "set-operation": {
      const left = pruneBelow(plan.left);
      return { ...plan, left, right: pruneBelow(plan.right) };
    }
    case "cte": {
      const definition = pruneBelow(plan.definition);
      return { ...plan, definition, body: pruned(plan.body, wanted) };
    }
    case "recursive-cte": {
      const anchor = pruneBelow(plan.anchor);
      return { ...plan, anchor, step: pruneBelow(plan.step) };
    }
  }
}
