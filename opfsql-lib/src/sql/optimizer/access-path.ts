import type { logical, OrderDirection, physical } from "../plan/index.js";
import {
  afterResidual,
  type Estimate,
  indexEstimate,
  orderedCost,
  rowidListEstimate,
  rowidRangeEstimate,
  rowsPassing,
  tableScanEstimate,
  unionEstimate,
} from "./cost.js";
import { conjunctsOf, disjunctsOf } from "./expression-tree.js";
import { type IndexKeys, isRowidOf, type KeyPart } from "./key-parts.js";
import {
  equalTestOn,
  fitsPart,
  isListTest,
  isRangeTest,
  type KeyTest,
  keyTestsAmong,
  type ListTest,
  type RangeTest,
  type SeekRange,
  type SeekValue,
} from "./key-tests.js";
import { deliveredDirection, type WantedOrder } from "./order-match.js";
import type { Planner } from "./planner.js";

// ---------------------------------------------------------------------------
// Pass 4, access paths: how a `get` reads the rows its filter conditions
// want. The candidates, cheapest first by cost.ts:
//
//   rowid-lookup   the rowid `=` or IN seek values, or a range of it;
//   index-scan     an index's leading parts each `=` a seek value, then at
//                  most one range on the next part (key-tests.ts says what
//                  fits a part); or the whole index, when it delivers the
//                  ORDER BY wanted;
//   index-union    an OR whose every branch has one of the above, or an
//                  IN whose every item does;
//   table-scan     every row, forward or backward for an ORDER BY rowid.
//
// A rowid lookup and a table scan read in rowid order or its reverse:
// either delivers an ORDER BY of the rowid, in its direction.
//
// Every candidate's cost is weighed with the order wanted (cost.ts): a
// path that delivers it saves the sort, and under a LIMIT stops early.
//
// Residual = every condition (docs/planning.md, rule 1), except the ones a
// path enforces exactly. A seek finds exactly the entries `=` or `<` holds
// for (key-tests.ts), so a test the path seeks with is left out — a range
// test only when the range uses every bound it has. An index union keeps
// its OR or IN; a condition used only to choose a path, or not used, stays.
// ---------------------------------------------------------------------------

type Expression = physical.Expression;

export interface ScanRequest {
  readonly get: logical.Get;
  /** The get's filter conditions, lowered. */
  readonly conditions: readonly Expression[];
  readonly order: WantedOrder | null;
}

export interface AccessPath {
  readonly plan: physical.Plan;
  /** True if its rows come in the order wanted. */
  readonly isOrdered: boolean;
  /** The rows that pass every condition, and what reading them costs. */
  readonly estimate: Estimate;
}

/** A read of a table's rows, which a residual can sit on. */
export type Scan =
  | physical.TableScan
  | physical.RowidLookup
  | physical.IndexScan
  | physical.IndexUnion;

export function isScan(plan: physical.Plan): plan is Scan {
  switch (plan.kind) {
    case "table-scan":
    case "rowid-lookup":
    case "index-scan":
    case "index-union": {
      return true;
    }
    case "index-extreme":
    case "filter":
    case "project":
    case "hash-join":
    case "nested-loop-join":
    case "lookup-join":
    case "hash-aggregate":
    case "sort":
    case "top-n":
    case "limit":
    case "hash-distinct":
    case "set-operation":
    case "cte":
    case "cte-scan":
    case "recursive-cte":
    case "values": {
      return false;
    }
  }
}

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown
  ? Omit<T, K>
  : never;

/** A scan without what every scan of the get has. */
type Access = DistributiveOmit<
  Scan,
  "tableIndex" | "table" | "columns" | "residual"
>;
export type RowidAccess = Extract<Access, { readonly kind: "rowid-lookup" }>;
export type IndexAccess = Extract<Access, { readonly kind: "index-scan" }>;

/** One way to read the rows. */
export interface Candidate<Kind extends Access = Access> {
  readonly access: Kind;
  /** Conditions it enforces exactly, which the residual leaves out. */
  readonly enforced: readonly Expression[];
  readonly estimate: Estimate;
  readonly isOrdered: boolean;
}

export function chooseAccessPath(
  request: ScanRequest,
  planner: Planner,
): AccessPath {
  const { get, conditions, order } = request;
  const tests = keyTestsAmong(conditions);
  const indexes = planner.indexes.indexesOf(get);
  const candidates = [
    rowidCandidate(get, tests, order),
    ...indexes.map((keys) => indexCandidate(keys, tests, order)),
    ...conditions.map((condition) => unionCandidate(condition, get, indexes)),
  ].filter((candidate) => candidate !== null);
  const scan = tableScanCandidate(request);
  const passing = rowsPassing(conditions);
  const best = cheapestBy([...candidates, scan], (candidate) => {
    if (order === null) return candidate.estimate.cost;
    const shape = {
      isDelivered: candidate.isOrdered,
      isLimited: order.isLimited,
    };
    return orderedCost(candidate.estimate, shape, passing);
  });
  const chosen = best ?? scan;
  return {
    plan: buildScan(chosen, request),
    isOrdered: chosen.isOrdered,
    estimate: afterResidual(chosen.estimate, conditions),
  };
}

/** The candidate `costOf` says is cheapest, the earlier on a tie; null if none. */
export function cheapestBy<C>(
  candidates: readonly C[],
  costOf: (candidate: C) => number,
): C | null {
  let best: C | null = null;
  let bestCost = Infinity;
  for (const candidate of candidates) {
    const cost = costOf(candidate);
    if (best !== null && cost >= bestCost) continue;
    best = candidate;
    bestCost = cost;
  }
  return best;
}

function buildScan(candidate: Candidate, request: ScanRequest): Scan {
  return { ...accessOf(candidate, request), ...candidate.access };
}

/** A seek's scan: a rowid lookup or an index scan. */
export function buildSeek(
  candidate: Candidate<RowidAccess | IndexAccess>,
  request: ScanRequest,
): physical.RowidLookup | physical.IndexScan {
  return { ...accessOf(candidate, request), ...candidate.access };
}

/** What every scan of the get has: its columns, and the conditions the path leaves. */
function accessOf(
  candidate: Candidate,
  request: ScanRequest,
): Omit<physical.TableScan, "kind" | "direction"> {
  const { tableIndex, table, columns } = request.get;
  const residual = request.conditions.filter(
    (condition) => !candidate.enforced.includes(condition),
  );
  return { tableIndex, table, columns, residual };
}

function tableScanCandidate(request: ScanRequest): Candidate {
  const read = rowidRead(rowidOrder(request.order, request.get));
  return {
    access: { kind: "table-scan", direction: read.direction },
    enforced: [],
    estimate: tableScanEstimate(),
    isOrdered: read.isOrdered,
  };
}

/** The direction of the order wanted if it starts with the rowid, else null. */
function rowidOrder(
  order: WantedOrder | null,
  get: logical.Get,
): OrderDirection | null {
  const first = order?.keys[0];
  if (first === undefined) return null;
  // Rowids are unique and never NULL: the keys after it change nothing.
  return isRowidOf(first.expression, get) ? first.direction : null;
}

// --- Rowids -------------------------------------------------------------------

/**
 * The rowids of the first `=` or IN on the rowid, else a range of them. A
 * lookup returns its rows in rowid order, or its reverse: it delivers an
 * ORDER BY of the rowid in either direction.
 */
export function rowidCandidate(
  get: logical.Get,
  tests: readonly KeyTest[],
  order: WantedOrder | null,
): Candidate<RowidAccess> | null {
  const fitting = tests.filter((keyTest) => isRowidOf(keyTest.subject, get));
  const read = rowidRead(rowidOrder(order, get));
  const listed = fitting.find((keyTest) => isListTest(keyTest));
  if (listed !== undefined) return rowidListCandidate(listed, read);
  return rowidRangeCandidate(fitting, read);
}

/** How a read in rowid order goes: in the direction wanted, if one is. */
interface RowidRead {
  readonly direction: OrderDirection;
  readonly isOrdered: boolean;
}

function rowidRead(wanted: OrderDirection | null): RowidRead {
  return { direction: wanted ?? "ascending", isOrdered: wanted !== null };
}

function rowidListCandidate(
  keyTest: ListTest,
  read: RowidRead,
): Candidate<RowidAccess> {
  const { test } = keyTest;
  const values = test.kind === "equal" ? [test.value] : test.values;
  const rowids = { kind: "list", values } as const;
  return {
    access: { kind: "rowid-lookup", rowids, direction: read.direction },
    enforced: [keyTest.condition],
    estimate: rowidListEstimate(values.length),
    isOrdered: read.isOrdered,
  };
}

function rowidRangeCandidate(
  fitting: readonly KeyTest[],
  read: RowidRead,
): Candidate<RowidAccess> | null {
  const ranged = rangeOn(fitting);
  if (ranged === null) return null;
  const rowids = { kind: "range", range: ranged.range } as const;
  return {
    access: { kind: "rowid-lookup", rowids, direction: read.direction },
    enforced: ranged.enforced,
    estimate: rowidRangeEstimate(ranged.range),
    isOrdered: read.isOrdered,
  };
}

// --- Indexes ------------------------------------------------------------------

/** An index seek, or the whole index if it delivers the order wanted; null if neither. */
export function indexCandidate(
  keys: IndexKeys,
  tests: readonly KeyTest[],
  order: WantedOrder | null,
): Candidate<IndexAccess> | null {
  const prefix = equalPrefix(keys.parts, tests);
  const ranged = rangeAfter(prefix, keys.parts, tests);
  const direction = deliveredDirection(order, keys.parts, prefix.values.length);
  const isSeek = prefix.values.length > 0 || ranged !== null;
  if (!isSeek && direction === null) return null;
  const range = ranged?.range ?? null;
  const isUniqueKey =
    keys.index.isUnique && prefix.values.length === keys.parts.length;
  return {
    access: {
      kind: "index-scan",
      index: keys.index,
      keys: { equal: prefix.values, range },
      direction: direction ?? "ascending",
    },
    enforced: [...prefix.enforced, ...(ranged?.enforced ?? [])],
    estimate: indexEstimate({
      equalParts: prefix.values.length,
      range,
      isUniqueKey,
    }),
    isOrdered: direction !== null,
  };
}

interface Prefix {
  readonly values: SeekValue[];
  readonly enforced: Expression[];
}

/** The leading parts that some test sets equal to a seek value, in order. */
function equalPrefix(
  parts: readonly KeyPart[],
  tests: readonly KeyTest[],
): Prefix {
  const prefix: Prefix = { values: [], enforced: [] };
  for (const part of parts) {
    const found = equalTestOn(part, tests);
    if (found === null) break;
    prefix.values.push(found.test.value);
    prefix.enforced.push(found.condition);
  }
  return prefix;
}

/** A range seek, and the conditions it enforces. */
interface RangeSeek {
  readonly range: SeekRange;
  readonly enforced: readonly Expression[];
}

/** The range the tests set on the part after the equality prefix, if any. */
function rangeAfter(
  prefix: Prefix,
  parts: readonly KeyPart[],
  tests: readonly KeyTest[],
): RangeSeek | null {
  const next = parts[prefix.values.length];
  if (next === undefined) return null;
  return rangeOn(tests.filter((keyTest) => fitsPart(keyTest, next)));
}

/** The range `fitting`, the tests that fit one part, set on it; null if they set none. */
function rangeOn(fitting: readonly KeyTest[]): RangeSeek | null {
  const ranges = fitting.filter((keyTest) => isRangeTest(keyTest));
  const bounds = boundsOf(ranges);
  if (bounds === null) return null;
  return { range: bounds.range, enforced: enforcedBy(bounds, ranges) };
}

/** A range, and the tests its lower and upper bounds came from. */
interface Bounds {
  readonly range: SeekRange;
  readonly lowerFrom: RangeTest | undefined;
  readonly upperFrom: RangeTest | undefined;
}

/** The first lower bound and the first upper bound among the tests. */
function boundsOf(ranges: readonly RangeTest[]): Bounds | null {
  const lowerFrom = ranges.find(({ test }) => test.range.lower !== null);
  const upperFrom = ranges.find(({ test }) => test.range.upper !== null);
  if (lowerFrom === undefined && upperFrom === undefined) return null;
  const lower = lowerFrom?.test.range.lower ?? null;
  const upper = upperFrom?.test.range.upper ?? null;
  return { range: { lower, upper }, lowerFrom, upperFrom };
}

/** The conditions a range enforces: those of the tests whose every bound it uses. */
function enforcedBy(
  bounds: Bounds,
  ranges: readonly RangeTest[],
): Expression[] {
  const isUsed = (keyTest: RangeTest): boolean => {
    const { lower, upper } = keyTest.test.range;
    const isLowerUsed = lower === null || keyTest === bounds.lowerFrom;
    const isUpperUsed = upper === null || keyTest === bounds.upperFrom;
    return isLowerUsed && isUpperUsed;
  };
  return ranges
    .filter((keyTest) => isUsed(keyTest))
    .map((keyTest) => keyTest.condition);
}

// --- OR and IN ------------------------------------------------------------------

/**
 * An index union for an OR whose every branch has a seek, or an IN whose
 * every item does (`x IN (a, b)` is `x = a OR x = b`, compared alike); it
 * enforces nothing, so the OR or IN stays in the residual.
 */
function unionCandidate(
  condition: Expression,
  get: logical.Get,
  indexes: readonly IndexKeys[],
): Candidate | null {
  const branches = alternativesOf(condition);
  if (branches.length < 2) return null;
  const seeks: Candidate<RowidAccess | IndexAccess>[] = [];
  for (const branch of branches) {
    const seek = branchSeek(branch, get, indexes);
    if (seek === null) return null;
    seeks.push(seek);
  }
  return {
    access: {
      kind: "index-union",
      branches: seeks.map((seek) => branchOf(seek.access)),
    },
    enforced: [],
    estimate: unionEstimate(seeks.map((seek) => seek.estimate)),
    isOrdered: false,
  };
}

/** The conditions any of which makes `condition` hold: an OR's operands, an IN's items. */
function alternativesOf(condition: Expression): Expression[] {
  if (condition.kind !== "in-list") return disjunctsOf(condition);
  const { needle } = condition;
  return condition.list.map((item) => ({
    kind: "comparison",
    operator: "EQUAL",
    left: needle,
    right: item,
    type: "BOOLEAN",
  }));
}

/** The cheapest rowid or index seek for one branch of an OR. */
function branchSeek(
  branch: Expression,
  get: logical.Get,
  indexes: readonly IndexKeys[],
): Candidate<RowidAccess | IndexAccess> | null {
  const tests = keyTestsAmong(conjunctsOf(branch));
  const seeks = [
    rowidCandidate(get, tests, null),
    ...indexes.map((keys) => indexCandidate(keys, tests, null)),
  ].filter((seek) => seek !== null);
  return cheapestBy(seeks, (seek) => seek.estimate.cost);
}

function branchOf(access: RowidAccess | IndexAccess): physical.IndexBranch {
  if (access.kind === "rowid-lookup") {
    return { kind: "rowids", rowids: access.rowids };
  }
  return { kind: "index", index: access.index, keys: access.keys };
}
