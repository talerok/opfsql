import { beforeEach, describe, expect, it } from "vitest";
import { SemanticError } from "../../errors.js";
import type { physical } from "../../sql/plan/index.js";
import {
  amount,
  type Fixture,
  fixture,
  id,
  orderId,
  ORDERS,
  orderUser,
  OTHER_USERS,
  SELECTED,
  USERS,
} from "./fixtures.js";
import {
  aggregateCall,
  arithmetic,
  column,
  compare,
  constant,
  Counting,
  cte,
  cteScan,
  exists,
  filter,
  hashAggregate,
  outer,
  project,
  recursiveCte,
  scalarSubquery,
  source,
  tableScan,
  values,
} from "./plans.js";

// Subqueries (docs/planning.md, Execution): compiled once with the plan
// they are part of; a correlated one started over for each outer row, any
// other run once.

// Table indexes of the relations these tests' subqueries make.
/** A subquery's select list. */
const INNER = 40;
/** A COUNT(*)'s groups; its aggregate is bound one index up. */
const COUNTED = 30;
/** A SUM's or AVG's groups; its aggregate one index up. */
const SUMMED = 32;
/** A CTE, and two scans of it. */
const MINE = 1;
const MINE_ROWS = 41;
const MINE_AGAIN = 44;
/** A recursive CTE: its result, its working set, and their relations. */
const SERIES = 1;
const WORKING_SET = 2;
const SERIES_RESULT = 60;
const ANCHOR = 61;
const STEP = 62;
const WORKING_ROWS = 63;
const SERIES_ROWS = 64;

let data: Fixture;

beforeEach(async () => {
  data = await fixture();
});

function usersScan(columns = [0, 1, 2]): physical.TableScan {
  return tableScan(source(data.users, USERS, columns));
}

function ordersScan(): physical.TableScan {
  return tableScan(source(data.orders, ORDERS, [0, 1, 2, 3]));
}

/** `SELECT id, <expression> FROM users`. */
function perUser(expression: physical.Expression): physical.Project {
  return project(SELECTED, usersScan(), id, expression);
}

/** COUNT(*) of `child`'s rows. */
function countOf(child: physical.Plan): physical.Plan {
  return hashAggregate(
    [COUNTED, COUNTED + 1],
    child,
    [],
    [aggregateCall("count", [])],
  );
}

describe("subqueries", () => {
  it("run a correlated subquery per outer row, compiled once, reading two levels out", () => {
    // SELECT id, (SELECT count(*) FROM orders o WHERE o.user = u.id AND
    //   EXISTS (SELECT 1 FROM users u2 WHERE u2.age = u.age AND u2.id <> o.user))
    // FROM users u: each user's orders, if another user shares their age.
    const counting = new Counting();
    const otherId = column(OTHER_USERS, 0, "INTEGER");
    const otherAge = column(OTHER_USERS, 2, "INTEGER");
    const sameAge = filter(
      tableScan(source(data.users, OTHER_USERS, [0, 1, 2])),
      compare(otherAge, "=", counting.of(outer(2, USERS, 2))),
      compare(otherId, "<>", outer(1, ORDERS, 1)),
    );
    const theirOrders = filter(
      ordersScan(),
      compare(orderUser, "=", outer(1, USERS, 0)),
      exists(sameAge, "correlated"),
    );
    const plan = perUser(scalarSubquery(countOf(theirOrders), "correlated"));
    expect(data.database.query(plan)).toEqual([
      [1, 2],
      [2, 0],
      [3, 0],
      [4, 0],
      [5, 0],
    ]);
    // Compiled once; run for each of users 1 and 2's orders, reading u.age
    // two levels out for each other user it looks at before it finds one:
    // 4 for each of Ann's two orders, 5 for bob's one.
    expect(counting.compiles).toBe(1);
    expect(counting.calls).toBe(13);
  });

  it("run a subquery without outer references once", () => {
    const counting = new Counting();
    const orders = project(INNER, ordersScan(), counting.of(orderId));
    const plan = perUser(scalarSubquery(countOf(orders)));
    expect(data.database.query(plan)).toEqual([
      [1, 5],
      [2, 5],
      [3, 5],
      [4, 5],
      [5, 5],
    ]);
    expect(counting.calls).toBe(5);
  });

  it("read one row for EXISTS, and fail a scalar subquery with two", () => {
    const counting = new Counting();
    const orders = project(INNER, ordersScan(), counting.of(orderId));
    expect(data.database.query(perUser(exists(orders)))).toEqual([
      [1, true],
      [2, true],
      [3, true],
      [4, true],
      [5, true],
    ]);
    // Not correlated, so run once; and for EXISTS, one row read.
    expect(counting.calls).toBe(1);
    const twoRows = perUser(
      scalarSubquery(project(INNER, ordersScan(), orderId)),
    );
    expect(() => data.database.query(twoRows)).toThrow(SemanticError);
  });

  it("run a CTE inside a correlated scalar subquery for each outer row", () => {
    // (WITH c AS (SELECT amount FROM orders WHERE user = u.id)
    //  SELECT sum(amount) FROM c)
    const theirs = filter(
      ordersScan(),
      compare(orderUser, "=", outer(1, USERS, 0)),
    );
    const amounts = project(INNER, theirs, amount);
    const total = hashAggregate(
      [SUMMED, SUMMED + 1],
      cteScan(MINE, MINE_ROWS, ["REAL"]),
      [],
      [aggregateCall("sum", [column(MINE_ROWS, 0, "REAL")])],
    );
    const plan = perUser(
      scalarSubquery(cte(MINE, amounts, total), "correlated"),
    );
    expect(data.database.query(plan)).toEqual([
      [1, 29.5],
      [2, 7],
      [3, null],
      [4, null],
      [5, null],
    ]);
  });

  it("let a correlated subquery read a CTE of the query around it", () => {
    // WITH big AS (SELECT user FROM orders WHERE amount > 5)
    // SELECT id, EXISTS (SELECT 1 FROM big WHERE big.user = u.id) FROM users u
    const big = project(
      INNER,
      filter(ordersScan(), compare(amount, ">", constant(5))),
      orderUser,
    );
    const theirs = filter(
      cteScan(MINE, MINE_ROWS, ["INTEGER"]),
      compare(column(MINE_ROWS, 0, "INTEGER"), "=", outer(1, USERS, 0)),
    );
    const plan = cte(MINE, big, perUser(exists(theirs, "correlated")));
    expect(data.database.query(plan)).toEqual([
      [1, true],
      [2, true],
      [3, false],
      [4, false],
      [5, false],
    ]);
  });

  it("run again a subquery that reads a CTE of a correlated level, marked correlated", () => {
    // SELECT id, (WITH mine AS (SELECT amount FROM orders WHERE user = u.id)
    //   SELECT count(*) FROM mine WHERE amount > (SELECT avg(amount) FROM mine))
    // FROM users u: the innermost subquery reads no outer column, but mine
    // changes with u, so the binder marks it correlated.
    data.database.insert(data.orders, [[15, 2, 1, null]]);
    const theirs = filter(
      ordersScan(),
      compare(orderUser, "=", outer(1, USERS, 0)),
    );
    const mine = project(INNER, theirs, amount);
    const average = hashAggregate(
      [SUMMED, SUMMED + 1],
      cteScan(MINE, MINE_AGAIN, ["REAL"]),
      [],
      [aggregateCall("avg", [column(MINE_AGAIN, 0, "REAL")])],
    );
    const above = filter(
      cteScan(MINE, MINE_ROWS, ["REAL"]),
      compare(
        column(MINE_ROWS, 0, "REAL"),
        ">",
        scalarSubquery(average, "correlated"),
      ),
    );
    const plan = perUser(
      scalarSubquery(cte(MINE, mine, countOf(above)), "correlated"),
    );
    // User 1's orders are 9.5 and 20, user 2's 7 and 1: one above each average.
    expect(data.database.query(plan)).toEqual([
      [1, 1],
      [2, 1],
      [3, 0],
      [4, 0],
      [5, 0],
    ]);
  });

  it("start a recursive CTE over for each outer row", () => {
    // (WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM r
    //  WHERE x < u.id) SELECT count(*) FROM r)
    const x = column(WORKING_ROWS, 0, "INTEGER");
    const working = cteScan(WORKING_SET, WORKING_ROWS, ["INTEGER"]);
    const step = project(
      STEP,
      filter(working, compare(x, "<", outer(1, USERS, 0))),
      arithmetic(x, "+", constant(1)),
    );
    const anchor = values(ANCHOR, [constant(1)]);
    const series = recursiveCte(
      [WORKING_SET, SERIES_RESULT],
      anchor,
      step,
      true,
    );
    const counted = cte(
      SERIES,
      series,
      countOf(cteScan(SERIES, SERIES_ROWS, ["INTEGER"])),
    );
    const plan = perUser(scalarSubquery(counted, "correlated"));
    expect(data.database.query(plan)).toEqual([
      [1, 1],
      [2, 2],
      [3, 3],
      [4, 4],
      [5, 5],
    ]);
  });
});
