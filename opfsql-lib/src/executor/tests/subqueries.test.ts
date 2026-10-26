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
  return project(9, usersScan(), id, expression);
}

/** COUNT(*) of `child`'s rows, bound to `groupIndex` and the next index. */
function countOf(child: physical.Plan, groupIndex: number): physical.Plan {
  return hashAggregate(
    [groupIndex, groupIndex + 1],
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
    const otherId = column(7, 0, "INTEGER");
    const otherAge = column(7, 2, "INTEGER");
    const sameAge = filter(
      tableScan(source(data.users, 7, [0, 1, 2])),
      compare(otherAge, "=", counting.of(outer(2, USERS, 2))),
      compare(otherId, "<>", outer(1, ORDERS, 1)),
    );
    const theirOrders = filter(
      ordersScan(),
      compare(orderUser, "=", outer(1, USERS, 0)),
      exists(sameAge, "correlated"),
    );
    const plan = perUser(
      scalarSubquery(countOf(theirOrders, 30), "correlated"),
    );
    expect(data.database.query(plan)).toEqual([
      [1, 2],
      [2, 0],
      [3, 0],
      [4, 0],
      [5, 0],
    ]);
    expect(counting.compiles).toBe(1);
    expect(counting.calls).toBeGreaterThan(1);
  });

  it("run a subquery without outer references once", () => {
    const counting = new Counting();
    const orders = project(40, ordersScan(), counting.of(orderId));
    const plan = perUser(scalarSubquery(countOf(orders, 41)));
    expect(data.database.query(plan).map(([, count]) => count)).toEqual([
      5, 5, 5, 5, 5,
    ]);
    expect(counting.calls).toBe(5);
  });

  it("read one row for EXISTS, and fail a scalar subquery with two", () => {
    const counting = new Counting();
    const orders = project(40, ordersScan(), counting.of(orderId));
    expect(data.database.query(perUser(exists(orders)))[0]).toEqual([1, true]);
    expect(counting.calls).toBe(1);
    const twoRows = perUser(scalarSubquery(project(40, ordersScan(), orderId)));
    expect(() => data.database.query(twoRows)).toThrow(SemanticError);
  });

  it("run a CTE inside a correlated scalar subquery for each outer row", () => {
    // (WITH c AS (SELECT amount FROM orders WHERE user = u.id)
    //  SELECT sum(amount) FROM c)
    const theirs = filter(
      ordersScan(),
      compare(orderUser, "=", outer(1, USERS, 0)),
    );
    const amounts = project(40, theirs, amount);
    const total = hashAggregate(
      [42, 43],
      cteScan(1, 41, ["REAL"]),
      [],
      [aggregateCall("sum", [column(41, 0, "REAL")])],
    );
    const plan = perUser(scalarSubquery(cte(1, amounts, total), "correlated"));
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
      44,
      filter(ordersScan(), compare(amount, ">", constant(5))),
      orderUser,
    );
    const theirs = filter(
      cteScan(2, 45, ["INTEGER"]),
      compare(column(45, 0, "INTEGER"), "=", outer(1, USERS, 0)),
    );
    const plan = cte(2, big, perUser(exists(theirs, "correlated")));
    expect(data.database.query(plan).map(([, has]) => has)).toEqual([
      true,
      true,
      false,
      false,
      false,
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
    const mine = project(40, theirs, amount);
    const average = hashAggregate(
      [45, 46],
      cteScan(1, 44, ["REAL"]),
      [],
      [aggregateCall("avg", [column(44, 0, "REAL")])],
    );
    const mineAmount = column(41, 0, "REAL");
    const above = filter(
      cteScan(1, 41, ["REAL"]),
      compare(mineAmount, ">", scalarSubquery(average, "correlated")),
    );
    const counted = countOf(above, 42);
    const plan = perUser(scalarSubquery(cte(1, mine, counted), "correlated"));
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
    const x = column(63, 0, "INTEGER");
    const step = project(
      62,
      filter(cteScan(2, 63, ["INTEGER"]), compare(x, "<", outer(1, USERS, 0))),
      arithmetic(x, "+", constant(1)),
    );
    const series = recursiveCte([2, 60], values(61, [constant(1)]), step, true);
    const counted = cte(1, series, countOf(cteScan(1, 64, ["INTEGER"]), 65));
    const plan = perUser(scalarSubquery(counted, "correlated"));
    expect(data.database.query(plan).map(([, count]) => count)).toEqual([
      1, 2, 3, 4, 5,
    ]);
  });
});
