import { beforeEach, describe, expect, it } from "vitest";
import { AbortError } from "../../errors.js";
import { cast } from "../../expressions/tests/builders.js";
import type { Table } from "../../schema/index.js";
import { column as columnOf } from "../../table/tests/database.js";
import { type Fixture, fixture, id, USERS } from "./fixtures.js";
import {
  arithmetic,
  column,
  compare,
  constant,
  Counting,
  cte,
  cteScan,
  exists,
  filter,
  limit,
  outer,
  project,
  recursiveCte,
  scalarSubquery,
  source,
  tableScan,
  values,
} from "./plans.js";

// An operator evaluates no more of its input than the rows it returns need
// (docs/planning.md, Execution): a row past a LIMIT, or past the one row
// EXISTS needs, is never evaluated and cannot fail the statement.

/** The table index of t (id INTEGER PRIMARY KEY, v TEXT) holding '1', '2', 'x'. */
const T = 5;
/** The table index of a projection over it. */
const PROJECTED = 6;

let data: Fixture;
let texts: Table;

beforeEach(async () => {
  data = await fixture();
  texts = data.database.createTable({
    name: "t",
    columns: [columnOf("id", "INTEGER"), columnOf("v", "TEXT")],
    rowidColumn: 0,
    isAutoincrement: false,
  });
  data.database.insert(texts, [
    [1, "1"],
    [2, "2"],
    [3, "x"],
  ]);
});

describe("rows past what is needed", () => {
  it("are never evaluated under a LIMIT: CAST('x' AS INTEGER) on row 3 never runs", () => {
    // SELECT CAST(v AS INTEGER) FROM t LIMIT 2
    const v = column(T, 1, "TEXT");
    const casts = project(
      PROJECTED,
      tableScan(source(texts, T, [0, 1])),
      cast(v, "INTEGER"),
    );
    expect(data.database.query(limit(casts, constant(2)))).toEqual([[1], [2]]);
  });

  it("are never evaluated in VALUES under a LIMIT", () => {
    // VALUES (1), (CAST('x' AS INTEGER)) LIMIT 1
    const rows = values(50, [constant(1)], [cast(constant("x"), "INTEGER")]);
    expect(data.database.query(limit(rows, constant(1)))).toEqual([[1]]);
  });

  it("are never evaluated for EXISTS, which needs one row", () => {
    const failing = values(50, [constant(1)], [cast(constant("x"), "INTEGER")]);
    const plan = project(9, values(51, []), exists(failing));
    expect(data.database.query(plan)).toEqual([[true]]);
  });

  it("leave a correlated subquery under LIMIT n run exactly n times", () => {
    const counting = new Counting();
    const perRow = values(52, [counting.of(outer(1, USERS, 0))]);
    const users = tableScan(source(data.users, USERS, [0]));
    const plan = project(9, users, id, scalarSubquery(perRow, "correlated"));
    expect(data.database.query(limit(plan, constant(2)))).toEqual([
      [1, 1],
      [2, 2],
    ]);
    expect(counting.calls).toBe(2);
  });

  it("leave a CTE nothing reads unrun, so it cannot fail the statement", () => {
    // WITH broken AS (SELECT (SELECT v FROM t)) SELECT 1
    const severalRows = project(
      40,
      tableScan(source(texts, T, [1])),
      column(T, 1, "TEXT"),
    );
    const broken = project(41, values(42, []), scalarSubquery(severalRows));
    const plan = cte(1, broken, values(43, [constant(1)]));
    expect(data.database.query(plan)).toEqual([[1]]);
  });
});

describe("checkpoints", () => {
  it("are where the engine can end a statement, an endless recursion included", () => {
    // WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM r) SELECT x FROM r
    const x = column(63, 0, "INTEGER");
    const step = project(
      62,
      cteScan(2, 63, ["INTEGER"]),
      arithmetic(x, "+", constant(1)),
    );
    const endless = recursiveCte(
      [2, 60],
      values(61, [constant(1)]),
      step,
      true,
    );
    const plan = cte(1, endless, cteScan(1, 64, ["INTEGER"]));
    let checkpoints = 0;
    const endAfterAThousand = () => {
      checkpoints++;
      if (checkpoints > 1000) throw new AbortError("ended by the engine");
    };
    expect(() => data.database.query(plan, [], endAfterAThousand)).toThrow(
      AbortError,
    );
    expect(checkpoints).toBe(1001);
  });

  it("come once per batch a scan returns", () => {
    let checkpoints = 0;
    const users = filter(
      tableScan(source(data.users, USERS, [0])),
      compare(id, ">", constant(0)),
    );
    data.database.query(users, [], () => {
      checkpoints++;
    });
    // One batch of five rows, then the pull that finds no more.
    expect(checkpoints).toBe(2);
  });
});
