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

// CTEs (docs/planning.md, Execution): a definition computed once and read
// by every scan of it; a recursive CTE as a `cte` whose definition is a
// `recursive-cte` with a working set of its own.

let data: Fixture;

beforeEach(async () => {
  data = await fixture();
});

/** The working set's one column, x, as the step reads it. */
const x = column(63, 0, "INTEGER");

/**
 * WITH RECURSIVE r(x) AS (anchor UNION [ALL] SELECT x + 1 FROM r [WHERE
 * x < below]) SELECT x FROM r [LIMIT count].
 */
function series(
  anchor: physical.Plan,
  isUnionAll: boolean,
  below: number | null,
  count: number | null = null,
): Value[] {
  const working = cteScan(2, 63, ["INTEGER"]);
  const read =
    below === null
      ? working
      : filter(working, compare(x, "<", constant(below)));
  const step = project(62, read, arithmetic(x, "+", constant(1)));
  const definition = recursiveCte([2, 60], anchor, step, isUnionAll);
  const result = cteScan(1, 64, ["INTEGER"]);
  const body = count === null ? result : limit(result, constant(count));
  const rows = data.database.query(cte(1, definition, body));
  return rows.map(([value = null]) => value);
}

describe("CTEs", () => {
  it("compute their definition once for every scan that reads it", () => {
    const counting = new Counting();
    const definition = project(
      40,
      tableScan(source(data.orders, ORDERS, [0])),
      counting.of(orderId),
    );
    const pairs = nestedLoopJoin(cteScan(1, 41), cteScan(1, 42));
    expect(data.database.query(cte(1, definition, pairs))).toHaveLength(25);
    expect(counting.calls).toBe(5);
  });

  it("recur with UNION ALL keeping duplicates, and UNION adding only new rows", () => {
    const twoOnes = values(61, [constant(1)], [constant(1)]);
    expect(series(twoOnes, true, 3)).toEqual([1, 1, 2, 2, 3, 3]);
    expect(series(twoOnes, false, 3)).toEqual([1, 2, 3]);
  });

  it("stop a UNION recursion at a cycle, when a step adds no row unseen", () => {
    // x + 1, wrapping at 3: 1, 2, 3, 1, … — UNION sees 1 again and stops.
    const working = cteScan(2, 63, ["INTEGER"]);
    const wrapped = arithmetic(
      arithmetic(x, "%", constant(3)),
      "+",
      constant(1),
    );
    const step = project(62, working, wrapped);
    const definition = recursiveCte(
      [2, 60],
      values(61, [constant(1)]),
      step,
      false,
    );
    const rows = data.database.query(cte(1, definition, cteScan(1, 64)));
    expect(rows).toEqual([[1], [2], [3]]);
  });

  it("read an endless recursion only as far as a LIMIT needs", () => {
    const one = values(61, [constant(1)]);
    expect(series(one, true, null, 4)).toEqual([1, 2, 3, 4]);
  });

  it("are pulled from a subquery as lazily as from their own query", () => {
    // WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM r)
    // SELECT (SELECT x FROM r LIMIT 1 OFFSET 5)
    const counting = new Counting();
    const working = cteScan(2, 63, ["INTEGER"]);
    const step = project(
      62,
      working,
      arithmetic(counting.of(x), "+", constant(1)),
    );
    const endless = recursiveCte(
      [2, 60],
      values(61, [constant(1)]),
      step,
      true,
    );
    const sixth = limit(cteScan(1, 64, ["INTEGER"]), constant(1), constant(5));
    const body = project(70, values(71, []), scalarSubquery(sixth));
    expect(data.database.query(cte(1, endless, body))).toEqual([[6]]);
    // The step ran on 1, 2, 3, 4 and 5: once for each row up to the sixth.
    expect(counting.calls).toBe(5);
  });
});
