import { beforeEach, describe, expect, it } from "vitest";
import type { physical } from "../../sql/plan/index.js";
import type { Value } from "../../values/index.js";
import { type Fixture, fixture, orderId, ORDERS } from "./fixtures.js";
import {
  arithmetic,
  column,
  compare,
  constant,
  Counting,
  cte,
  cteScan,
  filter,
  limit,
  nestedLoopJoin,
  project,
  recursiveCte,
  scalarSubquery,
  source,
  tableScan,
  values,
} from "./plans.js";

// CTEs (docs/planning.md, Execution): a definition computed once, as far as
// its scans read it; a recursive CTE as a `cte` whose definition is a
// `recursive-cte` with a working set of its own:
//
//   WITH RECURSIVE r(x) AS (anchor UNION [ALL] SELECT x + 1 FROM r …)

// Table indexes: the CTE and its working set, and the relations around them.
const SERIES = 1;
const WORKING_SET = 2;
const SERIES_RESULT = 60;
const ANCHOR = 61;
const STEP = 62;
const WORKING_ROWS = 63;
const SERIES_ROWS = 64;
const ORDER_IDS = 40;
const FIRST_SCAN = 41;
const SECOND_SCAN = 42;
const OUTER_SELECT = 70;
const NO_FROM = 71;

let data: Fixture;

beforeEach(async () => {
  data = await fixture();
});

/** x, the working set's one column, as the step reads it. */
const x = column(WORKING_ROWS, 0, "INTEGER");

/** The working set, as the step reads it. */
function workingSet(): physical.CteScan {
  return cteScan(WORKING_SET, WORKING_ROWS, ["INTEGER"]);
}

/** SELECT x + 1 FROM r, over `rows` of the working set. */
function nextOf(rows: physical.Plan, counting?: Counting): physical.Project {
  const read = counting === undefined ? x : counting.of(x);
  return project(STEP, rows, arithmetic(read, "+", constant(1)));
}

/** WITH RECURSIVE r(x) AS (anchor UNION [ALL] step) <body over r>. */
function recursion(
  anchor: physical.Plan,
  step: physical.Plan,
  isUnionAll: boolean,
  body: (series: physical.CteScan) => physical.Plan,
): Value[] {
  const definition = recursiveCte(
    [WORKING_SET, SERIES_RESULT],
    anchor,
    step,
    isUnionAll,
  );
  const series = cteScan(SERIES, SERIES_ROWS, ["INTEGER"]);
  const rows = data.database.query(cte(SERIES, definition, body(series)));
  return rows.map(([value = null]) => value);
}

/** `VALUES (v), …` as the anchor. */
function anchorOf(...items: number[]): physical.Values {
  return values(ANCHOR, ...items.map((item) => [constant(item)]));
}

/** The step counting on while x < 3. */
function countingToThree(): physical.Project {
  return nextOf(filter(workingSet(), compare(x, "<", constant(3))));
}

/** The first four rows of r. */
function firstFour(series: physical.CteScan): physical.Plan {
  return limit(series, constant(4));
}

/** SELECT (SELECT x FROM r LIMIT 1 OFFSET 5): the sixth row of r. */
function sixth(series: physical.CteScan): physical.Plan {
  const subquery = limit(series, constant(1), constant(5));
  return project(OUTER_SELECT, values(NO_FROM, []), scalarSubquery(subquery));
}

describe("CTEs", () => {
  it("compute their definition once for every scan that reads it", () => {
    const counting = new Counting();
    const orders = tableScan(source(data.orders, ORDERS, [0]));
    const definition = project(ORDER_IDS, orders, counting.of(orderId));
    const pairs = nestedLoopJoin(
      cteScan(SERIES, FIRST_SCAN),
      filter(
        cteScan(SERIES, SECOND_SCAN),
        compare(column(SECOND_SCAN, 0, "INTEGER"), "<", constant(12)),
      ),
    );
    const rows = data.database.query(cte(SERIES, definition, pairs));
    expect(rows).toEqual([
      [10, 10],
      [10, 11],
      [11, 10],
      [11, 11],
      [12, 10],
      [12, 11],
      [13, 10],
      [13, 11],
      [14, 10],
      [14, 11],
    ]);
    // Five orders, each computed once for both scans.
    expect(counting.calls).toBe(5);
  });

  it("recur with UNION ALL keeping duplicates, and UNION adding only new rows", () => {
    const keepAll = recursion(
      anchorOf(1, 1),
      countingToThree(),
      true,
      (r) => r,
    );
    expect(keepAll).toEqual([1, 1, 2, 2, 3, 3]);
    const newOnly = recursion(
      anchorOf(1, 1),
      countingToThree(),
      false,
      (r) => r,
    );
    expect(newOnly).toEqual([1, 2, 3]);
  });

  it("stop a UNION recursion at a cycle, when a step adds no row unseen", () => {
    // x % 3 + 1: 1, 2, 3, then 1 again, which UNION has seen.
    const wrapped = arithmetic(
      arithmetic(x, "%", constant(3)),
      "+",
      constant(1),
    );
    const step = project(STEP, workingSet(), wrapped);
    expect(recursion(anchorOf(1), step, false, (r) => r)).toEqual([1, 2, 3]);
  });

  it("read an endless recursion only as far as a LIMIT needs", () => {
    const counting = new Counting();
    const step = nextOf(workingSet(), counting);
    expect(recursion(anchorOf(1), step, true, firstFour)).toEqual([1, 2, 3, 4]);
    expect(counting.calls).toBe(3);
  });

  it("are pulled from a subquery as lazily as from their own query", () => {
    const counting = new Counting();
    const step = nextOf(workingSet(), counting);
    expect(recursion(anchorOf(1), step, true, sixth)).toEqual([6]);
    // The step ran on 1, 2, 3, 4 and 5: once for each row up to the sixth.
    expect(counting.calls).toBe(5);
  });
});
