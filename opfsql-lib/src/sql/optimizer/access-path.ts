import type { logical, OrderDirection, physical } from "../plan/index.js";
import {
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
import {
  type IndexKeys,
  indexesOf,
  isRowidOf,
  type KeyPart,
  rowidPart,
} from "./key-parts.js";
import {
  fitsPart,
  isExactRange,
  type KeyTest,
  keyTestOf,
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
// Every candidate's cost is weighed with the order wanted (cost.ts): a
// path that delivers it saves the sort, and under a LIMIT stops early.
//
// Residual = every condition (docs/planning.md, rule 1), except the ones a
// path enforces exactly. A seek finds exactly the entries `=` or `<` holds
// for (key-tests.ts), so a test the path seeks with is left out — unless it
// is part of a range open below on a part that may hold NULL, which starts
// at the NULL entries (isExactRange). An index union keeps its OR or IN; a
// condition used only to choose a path, or not used, stays.
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
}

type Scan =
  | physical.TableScan
  | physical.RowidLookup
  | physical.IndexScan
  | physical.IndexUnion;

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown
  ? Omit<T, K>
  : never;

/** A scan without what every scan of the get has. */
type Access = DistributiveOmit<
  Scan,
  "tableIndex" | "table" | "columns" | "residual"
>;
type RowidAccess = Extract<Access, { readonly kind: "rowid-lookup" }>;
type IndexAccess = Extract<Access, { readonly kind: "index-scan" }>;

/** One way to read the rows. */
interface Candidate<Kind extends Access = Access> {
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
  const tests = keyTestsOf(request.conditions);
  const indexes = indexesOf(request.get, planner);
  const candidates = [
    rowidCandidate(request.get, tests),
    ...indexes.map((keys) => indexCandidate(keys, tests, request.order)),
    ...request.conditions.map((condition) =>
      unionCandidate(condition, request.get, indexes),
    ),
  ].filter((candidate) => candidate !== null);
  const scan = tableScanCandidate(request);
  const best = cheapest([...candidates, scan], request) ?? scan;
  return { plan: scanOf(best, request), isOrdered: best.isOrdered };
}

function keyTestsOf(conditions: readonly Expression[]): KeyTest[] {
  return conditions.flatMap((condition) => keyTestOf(condition) ?? []);
}

/** The cheapest candidate, the earlier on a tie. */
function cheapest(
  candidates: readonly Candidate[],
  request: ScanRequest,
): Candidate | null {
  const passing = rowsPassing(request.conditions);
  const costOf = (candidate: Candidate): number => {
    const { order } = request;
    if (order === null) return candidate.estimate.cost;
    const shape = {
      isDelivered: candidate.isOrdered,
      isLimited: order.isLimited,
    };
    return orderedCost(candidate.estimate, shape, passing);
  };
  let best: Candidate | null = null;
  for (const candidate of candidates) {
    if (best === null || costOf(candidate) < costOf(best)) best = candidate;
  }
  return best;
}

function scanOf(candidate: Candidate, request: ScanRequest): Scan {
  const { tableIndex, table, columns } = request.get;
  const residual = request.conditions.filter(
    (condition) => !candidate.enforced.includes(condition),
  );
  return { tableIndex, table, columns, residual, ...candidate.access };
}

function tableScanCandidate(request: ScanRequest): Candidate {
  const direction = rowidOrder(request);
  return {
    access: { kind: "table-scan", direction: direction ?? "ascending" },
    enforced: [],
    estimate: tableScanEstimate(),
    isOrdered: direction !== null,
  };
}

/** The direction a table scan delivers the order wanted in, if it starts with the rowid. */
function rowidOrder(request: ScanRequest): OrderDirection | null {
  const first = request.order?.keys[0];
  if (first === undefined) return null;
  // Rowids are unique and never NULL: the keys after it change nothing.
  return isRowidOf(first.expression, request.get) ? first.direction : null;
}

// --- Rowids -------------------------------------------------------------------

/** The listed rowids of the first `=` or IN on the rowid, or a range of them. */
function rowidCandidate(
  get: logical.Get,
  tests: readonly KeyTest[],
): Candidate<RowidAccess> | null {
  const part = rowidPart(get);
  const fitting = tests.filter((keyTest) => isRowidOf(keyTest.subject, get));
  for (const keyTest of fitting) {
    const { test } = keyTest;
    if (test.kind === "range") continue;
    const values = test.kind === "equal" ? [test.value] : test.values;
    return {
      access: { kind: "rowid-lookup", rowids: { kind: "list", values } },
      enforced: [keyTest.condition],
      estimate: rowidListEstimate(values.length),
      isOrdered: false,
    };
  }
  const ranged = rangeOn(part, fitting);
  if (ranged === null) return null;
  const rowids = { kind: "range", range: ranged.range } as const;
  return {
    access: { kind: "rowid-lookup", rowids },
    enforced: ranged.enforced,
    estimate: rowidRangeEstimate(ranged.range),
    isOrdered: false,
  };
}

// --- Indexes ------------------------------------------------------------------

/** An index seek, or the whole index if it delivers the order wanted; null if neither. */
function indexCandidate(
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
    prefix.values.push(found.value);
    prefix.enforced.push(found.keyTest.condition);
  }
  return prefix;
}

/** The range the tests set on the part after the equality prefix, if any. */
function rangeAfter(
  prefix: Prefix,
  parts: readonly KeyPart[],
  tests: readonly KeyTest[],
): ReturnType<typeof rangeOn> {
  const next = parts[prefix.values.length];
  if (next === undefined) return null;
  return rangeOn(
    next,
    tests.filter((keyTest) => fitsPart(keyTest, next)),
  );
}

function equalTestOn(
  part: KeyPart,
  tests: readonly KeyTest[],
): { readonly keyTest: KeyTest; readonly value: SeekValue } | null {
  for (const keyTest of tests) {
    const { test } = keyTest;
    if (test.kind === "equal" && fitsPart(keyTest, part)) {
      return { keyTest, value: test.value };
    }
  }
  return null;
}

/**
 * The range `fitting`, the tests that fit `part`, set on it: the first
 * lower bound and the first upper bound among them. A test is enforced
 * when it is exact and every bound it has was used.
 */
function rangeOn(
  part: KeyPart,
  fitting: readonly KeyTest[],
): { readonly range: SeekRange; readonly enforced: Expression[] } | null {
  const ranges: { readonly keyTest: KeyTest; readonly range: SeekRange }[] = [];
  for (const keyTest of fitting) {
    const { test } = keyTest;
    if (test.kind === "range") ranges.push({ keyTest, range: test.range });
  }
  const lower = ranges.find(({ range }) => range.lower !== null);
  const upper = ranges.find(({ range }) => range.upper !== null);
  if (lower === undefined && upper === undefined) return null;
  const range = {
    lower: lower?.range.lower ?? null,
    upper: upper?.range.upper ?? null,
  };
  const isFullyUsed = (candidate: (typeof ranges)[number]): boolean =>
    (candidate.range.lower === null || candidate === lower) &&
    (candidate.range.upper === null || candidate === upper);
  const enforced = isExactRange(range, part)
    ? ranges
        .filter((candidate) => isFullyUsed(candidate))
        .map(({ keyTest }) => keyTest.condition)
    : [];
  return { range, enforced };
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
  const tests = keyTestsOf(conjunctsOf(branch));
  const seeks = [
    rowidCandidate(get, tests),
    ...indexes.map((keys) => indexCandidate(keys, tests, null)),
  ].filter((seek) => seek !== null);
  let best: Candidate<RowidAccess | IndexAccess> | null = null;
  for (const seek of seeks) {
    if (best === null || seek.estimate.cost < best.estimate.cost) best = seek;
  }
  return best;
}

function branchOf(access: RowidAccess | IndexAccess): physical.IndexBranch {
  if (access.kind === "rowid-lookup") {
    return { kind: "rowids", rowids: access.rowids };
  }
  return { kind: "index", index: access.index, keys: access.keys };
}
