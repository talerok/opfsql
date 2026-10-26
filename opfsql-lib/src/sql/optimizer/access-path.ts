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
  isExact,
  type KeyTest,
  keyTestOf,
  type SeekRange,
  type SeekValue,
  seeksRowids,
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
//   index-union    an OR whose every branch has one of the above;
//   table-scan     every row, forward or backward for an ORDER BY rowid.
//
// Every candidate's cost is weighed with the order wanted (cost.ts): a
// path that delivers it saves the sort, and under a LIMIT stops early.
//
// Residual = every condition (docs/planning.md, rule 1), except the ones a
// path is proven to enforce exactly (key-tests.ts, isExact): an `=`, IN or
// lower bound with constants, of a type the part does not convert, and an
// upper bound too where the part holds no NULL. A condition with a
// parameter always stays; an index union keeps its OR; a condition used
// only to choose a path stays.
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

function rowidCandidate(
  get: logical.Get,
  tests: readonly KeyTest[],
): Candidate<RowidAccess> | null {
  const fitting = tests.filter(
    (test) => isRowidOf(test.subject, get) && seeksRowids(test.test),
  );
  const part = rowidPart(get);
  for (const keyTest of fitting) {
    const values = listedValues(keyTest);
    if (values === null) continue;
    return {
      access: { kind: "rowid-lookup", rowids: { kind: "list", values } },
      enforced: isExact(keyTest, part) ? [keyTest.condition] : [],
      estimate: rowidListEstimate(values.length),
      isOrdered: false,
    };
  }
  const ranged = rangeOn(part, fitting);
  if (ranged === null) return null;
  return {
    access: {
      kind: "rowid-lookup",
      rowids: { kind: "range", range: ranged.range },
    },
    enforced: ranged.enforced,
    estimate: rowidRangeEstimate(ranged.range),
    isOrdered: false,
  };
}

function listedValues(keyTest: KeyTest): readonly SeekValue[] | null {
  const { test } = keyTest;
  if (test.kind === "equal") return [test.value];
  return test.kind === "in" ? test.values : null;
}

// --- Indexes ------------------------------------------------------------------

/** An index seek, or the whole index if it delivers the order wanted; null if neither. */
function indexCandidate(
  keys: IndexKeys,
  tests: readonly KeyTest[],
  order: WantedOrder | null,
): Candidate<IndexAccess> | null {
  const prefix = equalPrefix(keys.parts, tests);
  const next = keys.parts[prefix.values.length];
  const ranged =
    next === undefined
      ? null
      : rangeOn(
          next,
          tests.filter((keyTest) => fitsPart(keyTest, next)),
        );
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
    if (isExact(found.keyTest, part))
      prefix.enforced.push(found.keyTest.condition);
  }
  return prefix;
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
  const enforced = ranges
    .filter(
      (candidate) => isFullyUsed(candidate) && isExact(candidate.keyTest, part),
    )
    .map(({ keyTest }) => keyTest.condition);
  return { range, enforced };
}

// --- OR -------------------------------------------------------------------------

/** An index union for an OR whose every branch has a seek; it enforces nothing. */
function unionCandidate(
  condition: Expression,
  get: logical.Get,
  indexes: readonly IndexKeys[],
): Candidate | null {
  const branches = disjunctsOf(condition);
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
