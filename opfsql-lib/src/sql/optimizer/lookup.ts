import type { logical, physical } from "../plan/index.js";
import {
  buildScan,
  type Candidate,
  cheapestBy,
  type IndexAccess,
  indexCandidate,
  type RowidAccess,
  rowidCandidate,
} from "./access-path.js";
import { afterResidual, type Estimate } from "./cost.js";
import { keyTestsAmong } from "./key-tests.js";
import type { Planner } from "./planner.js";

// ---------------------------------------------------------------------------
// A lookup join's right side (docs/planning.md, Physical plans): one table
// read by a seek that starts again for each left row, its seek values read
// from that row. The tests it may seek with are the read's own conditions
// and the join's, the left side's columns counting as seek values there
// (key-tests.ts), with the same exactness as any seek: a test the seek uses
// leaves its residual (rule 1). The read's own conditions it does not use
// stay on the scan, as they read only its columns; the join conditions it
// does not use become the join's residual, checked on each pair (rule 2).
//
// Only a rowid lookup or an index scan whose seek reads the left row
// qualifies: a seek on constants alone finds the same rows for every left
// row, which a join reading the right side once does better.
// ---------------------------------------------------------------------------

type Expression = physical.Expression;
type Seek = Candidate<RowidAccess> | Candidate<IndexAccess>;

export interface LookupRequest {
  readonly get: logical.Get;
  /** The read's own filter conditions, lowered: they read only its columns. */
  readonly conditions: readonly Expression[];
  /** The join's conditions, lowered. */
  readonly joinConditions: readonly Expression[];
  /** The left side's tables, whose columns are seek values here. */
  readonly leftTables: ReadonlySet<number>;
}

export interface Lookup {
  readonly scan: physical.RowidLookup | physical.IndexScan;
  /** The join conditions the seek does not enforce exactly: the join's residual. */
  readonly residual: readonly Expression[];
  /** One seek: the rows it finds for a left row, and what it costs. */
  readonly estimate: Estimate;
}

/** The cheapest seek of the get by values of the left row, or null if none. */
export function chooseLookup(
  request: LookupRequest,
  planner: Planner,
): Lookup | null {
  const { get, conditions, joinConditions, leftTables } = request;
  const tests = keyTestsAmong([...conditions, ...joinConditions], leftTables);
  const seeks: Seek[] = [
    rowidCandidate(get, tests, null),
    ...planner.indexes
      .indexesOf(get)
      .map((keys) => indexCandidate(keys, tests, null)),
  ].filter(
    (seek): seek is Seek => seek !== null && seeksOnLeftRow(seek.access),
  );
  const best = cheapestBy(seeks, (seek) => seek.estimate.cost);
  if (best === null) return null;
  const scan = buildScan<RowidAccess | IndexAccess>(best, {
    get,
    conditions,
    order: null,
  });
  const residual = joinConditions.filter(
    (condition) => !best.enforced.includes(condition),
  );
  return { scan, residual, estimate: afterResidual(best.estimate, conditions) };
}

/** True if a seek value reads the left row: a column of this query. */
function seeksOnLeftRow(access: RowidAccess | IndexAccess): boolean {
  return seekValuesOf(access).some(
    (value) => value.kind === "column" && value.depth === 0,
  );
}

/** A seek: an index scan's key range, or a rowid lookup's rowids. */
type Seeking =
  | Pick<physical.IndexScan, "kind" | "keys">
  | Pick<physical.RowidLookup, "kind" | "rowids">;

/** The values a seek reads, each fixed for one scan. */
export function seekValuesOf(seek: Seeking): Expression[] {
  if (seek.kind === "index-scan") {
    const { equal, range } = seek.keys;
    return [...equal, ...boundValues(range)];
  }
  const { rowids } = seek;
  if (rowids.kind === "list") return [...rowids.values];
  return boundValues(rowids.range);
}

function boundValues(range: physical.Range | null): Expression[] {
  if (range === null) return [];
  const bounds = [range.lower, range.upper];
  return bounds.flatMap((bound) => (bound === null ? [] : [bound.value]));
}
