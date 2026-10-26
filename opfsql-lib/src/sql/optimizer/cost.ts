import type { physical } from "../plan/index.js";

// ---------------------------------------------------------------------------
// The cost model, the one place that guesses. There are no statistics yet:
// every table is assumed to hold ASSUMED_TABLE_ROWS rows, every kind of
// condition to keep a fixed share of them, and a LIMIT to want
// ASSUMED_LIMIT_ROWS (its value may be a parameter, which never decides a
// plan). Statistics plug in here: row counts per table, selectivities per
// index, and nothing outside this file changes.
//
// A cost is roughly the rows touched, a row read through an index counting
// twice (its entry, then the row). With these numbers the access paths
// order as docs/planning.md asks, for any table size:
//
//   rowid equality              1 per value
//   unique index, whole key     2
//   index equality prefix       2 × (1 + rows), fewer rows for a longer prefix
//   range on rowid / an index   1 / 2 × (1 + rows), a third or a tenth of them
//   table scan                  every row
//
// Joins (docs/planning.md, Physical plans) add one guess: starting a seek —
// a descent from the root — costs SEEK_START_COST rows besides the rows it
// reads, which a scan read once does not pay. So a lookup join costs its
// left side plus, for each left row, a seek's start and its rows; a hash
// join or a nested loop reads both sides once, a nested loop then pairing
// every row with every row. Each left row meets as many right rows as a
// seek on the join's key would find there. With these, a side found by a
// rowid, a unique key or an equality leads and the other is looked up;
// two sides that both need a full read meet in a hash join.
// ---------------------------------------------------------------------------

const ASSUMED_TABLE_ROWS = 10_000;
const ASSUMED_LIMIT_ROWS = 100;

const EQUALITY_SELECTIVITY = 0.01;
const ONE_BOUND_SELECTIVITY = 0.3;
const TWO_BOUND_SELECTIVITY = 0.1;
/** A condition no seek helps with, judged blind. */
const OTHER_SELECTIVITY = 0.5;

const ROW_COST = 1;
/** An index entry, then its row by rowid. */
const INDEX_ROW_COST = 2;
/** Sorting, per row: a flat stand-in for the comparisons. */
const SORT_ROW_COST = 1;
/** Starting a seek, a descent from the root, besides the rows it reads. */
const SEEK_START_COST = 2;

/** What a read is expected to touch, and what that costs. */
export interface Estimate {
  readonly rows: number;
  readonly cost: number;
}

export function tableScanEstimate(): Estimate {
  return { rows: ASSUMED_TABLE_ROWS, cost: ASSUMED_TABLE_ROWS * ROW_COST };
}

/** A plan read as a whole that is no table read: a derived table, VALUES. */
export function unknownEstimate(): Estimate {
  return tableScanEstimate();
}

/** A plan that reads nothing. */
export const NOTHING: Estimate = { rows: 0, cost: 0 };

/**
 * A read's estimate with its residual applied: the rows that pass both
 * what the path found and every condition.
 */
export function afterResidual(
  estimate: Estimate,
  conditions: readonly physical.Expression[],
): Estimate {
  return {
    ...estimate,
    rows: Math.min(estimate.rows, rowsPassing(conditions)),
  };
}

/** A lookup join: the left side once, then for each left row a seek. */
export function lookupJoinEstimate(left: Estimate, seek: Estimate): Estimate {
  return {
    rows: left.rows * seek.rows,
    cost: left.cost + left.rows * (SEEK_START_COST + seek.cost),
  };
}

/** A hash join: both sides once; `matches` right rows for each left row. */
export function hashJoinEstimate(
  left: Estimate,
  right: Estimate,
  matches: number,
): Estimate {
  return { rows: left.rows * matches, cost: left.cost + right.cost };
}

/** A nested loop: both sides once, then every pair checked. */
export function nestedLoopEstimate(left: Estimate, right: Estimate): Estimate {
  const pairs = left.rows * right.rows;
  return { rows: pairs, cost: left.cost + right.cost + pairs * ROW_COST };
}

/** The right rows a left row meets on `keys` equalities, with no seek to ask. */
export function keyMatches(right: Estimate, keys: number): number {
  return right.rows * EQUALITY_SELECTIVITY ** keys;
}

export function rowidListEstimate(count: number): Estimate {
  return { rows: count, cost: count * ROW_COST };
}

export function rowidRangeEstimate(range: physical.Range): Estimate {
  const rows = ASSUMED_TABLE_ROWS * rangeSelectivity(range);
  return { rows, cost: (1 + rows) * ROW_COST };
}

/** The shape of an index seek: what the estimate depends on. */
export interface IndexSeek {
  readonly equalParts: number;
  readonly range: physical.Range | null;
  /** Every part of a unique index is equal: one row at most. */
  readonly isUniqueKey: boolean;
}

export function indexEstimate(seek: IndexSeek): Estimate {
  if (seek.isUniqueKey) return { rows: 1, cost: INDEX_ROW_COST };
  const equal = EQUALITY_SELECTIVITY ** seek.equalParts;
  const range = seek.range === null ? 1 : rangeSelectivity(seek.range);
  const rows = ASSUMED_TABLE_ROWS * equal * range;
  return { rows, cost: (1 + rows) * INDEX_ROW_COST };
}

export function unionEstimate(branches: readonly Estimate[]): Estimate {
  let rows = 0;
  let cost = 0;
  for (const branch of branches) {
    rows += branch.rows;
    cost += branch.cost;
  }
  return { rows, cost };
}

function rangeSelectivity(range: physical.Range): number {
  const isClosed = range.lower !== null && range.upper !== null;
  return isClosed ? TWO_BOUND_SELECTIVITY : ONE_BOUND_SELECTIVITY;
}

/** The rows of a table that pass `conditions`. */
export function rowsPassing(
  conditions: readonly physical.Expression[],
): number {
  let rows = ASSUMED_TABLE_ROWS;
  for (const condition of conditions) rows *= selectivityOf(condition);
  return rows;
}

function selectivityOf(condition: physical.Expression): number {
  if (condition.kind === "between") return TWO_BOUND_SELECTIVITY;
  if (condition.kind === "in-list") {
    return Math.min(1, condition.list.length * EQUALITY_SELECTIVITY);
  }
  if (condition.kind !== "comparison") return OTHER_SELECTIVITY;
  if (condition.operator === "EQUAL") return EQUALITY_SELECTIVITY;
  const isRange = condition.operator !== "NOT_EQUAL";
  return isRange ? ONE_BOUND_SELECTIVITY : OTHER_SELECTIVITY;
}

/**
 * What a read costs once its rows are in the order wanted: as it is if it
 * delivers that order — less under a LIMIT, which stops it once enough of
 * `passing` rows came out — or plus a sort.
 */
export function orderedCost(
  estimate: Estimate,
  order: { readonly isDelivered: boolean; readonly isLimited: boolean },
  passing: number,
): number {
  if (!order.isDelivered) return estimate.cost + passing * SORT_ROW_COST;
  if (!order.isLimited) return estimate.cost;
  const share = Math.min(1, ASSUMED_LIMIT_ROWS / Math.max(passing, 1));
  return estimate.cost * share;
}
