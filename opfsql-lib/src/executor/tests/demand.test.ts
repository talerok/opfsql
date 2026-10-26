import { beforeEach, describe, expect, it } from "vitest";
import { AbortError } from "../../errors.js";
import { cast } from "../../expressions/tests/builders.js";
import type { Table } from "../../schema/index.js";
import { column as columnOf } from "../../table/tests/database.js";
import {
  type Fixture,
  fixture,
  id,
  ORDERS,
  orderUser,
  SELECTED,
  USERS,
} from "./fixtures.js";
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
  hashJoin,
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
/** Table indexes of the select lists, VALUES and CTEs these tests make. */
const PROJECTED = 6;
const LIST = 50;
const NO_FROM = 51;
const PER_ROW = 52;
const BROKEN = 1;
const SUBQUERY_SELECT = 40;
const CTE_SELECT = 41;
const SERIES = 1;
const WORKING_SET = 2;
const SERIES_RESULT = 60;
const ANCHOR = 61;
const STEP = 62;
const WORKING_ROWS = 63;
const SERIES_ROWS = 64;
const COUNTED_USERS = 7;

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

/** users JOIN orders ON id = user, its left side's ids counted as they are computed. */
function countedJoin(counting: Counting): ReturnType<typeof hashJoin> {
  const users = tableScan(source(data.users, USERS, [0]));
  const counted = project(COUNTED_USERS, users, counting.of(id));
  const orders = tableScan(source(data.orders, ORDERS, [0, 1]));
  const userId = column(COUNTED_USERS, 0, "INTEGER");
  return hashJoin([counted, orders], [[userId], [orderUser]]);
}

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
    const rows = values(LIST, [constant(1)], [cast(constant("x"), "INTEGER")]);
    expect(data.database.query(limit(rows, constant(1)))).toEqual([[1]]);
  });

  it("are never evaluated for EXISTS, which needs one row", () => {
    const failing = values(
      LIST,
      [constant(1)],
      [cast(constant("x"), "INTEGER")],
    );
    const plan = project(SELECTED, values(NO_FROM, []), exists(failing));
    expect(data.database.query(plan)).toEqual([[true]]);
  });

  it("leave a correlated subquery under LIMIT n run exactly n times", () => {
    const counting = new Counting();
    const perRow = values(PER_ROW, [counting.of(outer(1, USERS, 0))]);
    const users = tableScan(source(data.users, USERS, [0]));
    const plan = project(
      SELECTED,
      users,
      id,
      scalarSubquery(perRow, "correlated"),
    );
    expect(data.database.query(limit(plan, constant(2)))).toEqual([
      [1, 1],
      [2, 2],
    ]);
    expect(counting.calls).toBe(2);
  });

  it("leave a CTE nothing reads unrun, so it cannot fail the statement", () => {
    // WITH broken AS (SELECT (SELECT v FROM t)) SELECT 1
    const severalRows = project(
      SUBQUERY_SELECT,
      tableScan(source(texts, T, [1])),
      column(T, 1, "TEXT"),
    );
    const broken = project(
      CTE_SELECT,
      values(NO_FROM, []),
      scalarSubquery(severalRows),
    );
    const plan = cte(BROKEN, broken, values(LIST, [constant(1)]));
    expect(data.database.query(plan)).toEqual([[1]]);
  });
});

describe("joins", () => {
  it("take their left side a row at a time under a LIMIT", () => {
    const counting = new Counting();
    const first = limit(countedJoin(counting), constant(1));
    expect(data.database.query(first)).toEqual([[1, 10, 1]]);
    expect(counting.calls).toBe(1);
  });

  it("take their left side in full batches when every row is wanted", () => {
    const counting = new Counting();
    let checkpoints = 0;
    const rows = data.database.query(countedJoin(counting), [], () => {
      checkpoints++;
    });
    expect(rows).toEqual([
      [1, 10, 1],
      [1, 11, 1],
      [2, 12, 2],
    ]);
    // Each scan: one batch of every row, then the pull that finds no more.
    expect(checkpoints).toBe(4);
    expect(counting.calls).toBe(5);
  });
});

describe("checkpoints", () => {
  it("are where the engine can end a statement, an endless recursion included", () => {
    // WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM r) SELECT x FROM r
    const x = column(WORKING_ROWS, 0, "INTEGER");
    const step = project(
      STEP,
      cteScan(WORKING_SET, WORKING_ROWS, ["INTEGER"]),
      arithmetic(x, "+", constant(1)),
    );
    const endless = recursiveCte(
      [WORKING_SET, SERIES_RESULT],
      values(ANCHOR, [constant(1)]),
      step,
      true,
    );
    const plan = cte(
      SERIES,
      endless,
      cteScan(SERIES, SERIES_ROWS, ["INTEGER"]),
    );
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
