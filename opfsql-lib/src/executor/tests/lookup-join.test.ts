import { beforeEach, describe, expect, it } from "vitest";
import type { physical } from "../../sql/plan/index.js";
import type { Value } from "../../values/index.js";
import {
  age,
  amount,
  type Fixture,
  fixture,
  id,
  name,
  ORDERS,
  orderUser,
  OTHER_USERS,
  SELECTED,
  USERS,
} from "./fixtures.js";
import {
  aggregateCall,
  arithmetic,
  ascending,
  between,
  column,
  compare,
  constant,
  Counting,
  exists,
  excluding,
  filter,
  hashAggregate,
  hashJoin,
  indexScan,
  keys,
  limit,
  lookupJoin,
  nestedLoopJoin,
  outer,
  parameter,
  project,
  rowidList,
  scalarSubquery,
  sort,
  source,
  tableScan,
} from "./plans.js";

// A lookup join (docs/planning.md, Physical plans): for each left row, the
// right scan seeks with values read from that row. Each test holds it
// against a hash join or a nested loop over the same data, which read the
// whole right side; a lookup join must give the same rows, in the left
// side's order.

/** The other side of a self-join of users, and its columns. */
const otherId = column(OTHER_USERS, 0, "INTEGER");
const otherName = column(OTHER_USERS, 1, "TEXT");
const otherAge = column(OTHER_USERS, 2, "INTEGER");
/** Table indexes of a counted projection and of an aggregate's output. */
const COUNTED_ORDERS = 7;
const GROUPS = 20;
const AGGREGATES = 21;

let data: Fixture;

beforeEach(async () => {
  data = await fixture();
});

function query(plan: physical.Plan, parameters: Value[] = []): Value[][] {
  return data.database.query(plan, parameters);
}

function ordersScan(): physical.TableScan {
  return tableScan(source(data.orders, ORDERS, [0, 1, 2]));
}

function usersScan(direction: physical.TableScan["direction"] = "ascending") {
  return tableScan(source(data.users, USERS, [0, 1, 2]), { direction });
}

/** The other users, every row, for the joins that read the whole right side. */
function otherUsers(): physical.TableScan {
  return tableScan(source(data.users, OTHER_USERS, [0, 1, 2]));
}

/** The right side's first column of a joined row: after three of the left's. */
function userIdOf(row: readonly Value[]): Value {
  return row[3] ?? null;
}

/** COUNT(*) of a plan's rows. */
function countOf(plan: physical.Plan): physical.HashAggregate {
  return hashAggregate(
    [GROUPS, AGGREGATES],
    plan,
    [],
    [aggregateCall("count", [])],
  );
}

/** Each user's id, and how many rows `join` gives as its correlated subquery. */
function perUser(join: physical.Plan): Value[][] {
  const counted = scalarSubquery(countOf(join), "correlated");
  return query(project(SELECTED, usersScan(), id, counted));
}

/** Each order's user, found by rowid: the order's user column is the seek. */
function userOfEachOrder(): physical.RowidLookup {
  return rowidList(source(data.users, USERS, [0, 1, 2]), [orderUser]);
}

describe("a lookup join by rowid", () => {
  const byUser: [physical.Expression[], physical.Expression[]] = [
    [orderUser],
    [id],
  ];

  it.each(["inner", "left"] as const)(
    "gives a %s join's rows, a NULL or missing key matching nothing",
    (joinType) => {
      const looked = query(
        lookupJoin(ordersScan(), userOfEachOrder(), { joinType }),
      );
      const hashed = query(
        hashJoin([ordersScan(), usersScan()], byUser, { joinType }),
      );
      expect(looked).toEqual(hashed);
      // Order 13's user is NULL and order 14's user 9 does not exist.
      const padded = joinType === "left" ? [13, 14] : [];
      const unmatched = looked.filter((row) => userIdOf(row) === null);
      expect(unmatched.map(([orderId]) => orderId)).toEqual(padded);
    },
  );

  it.each(["inner", "left"] as const)(
    "checks its residual on each pair, a %s join padding a row none passes",
    (joinType) => {
      // ON user = id AND amount * 3 > age: order 11 passes, 10 and 12 do not.
      const residual = [
        compare(arithmetic(amount, "*", constant(3)), ">", age),
      ];
      const looked = query(
        lookupJoin(ordersScan(), userOfEachOrder(), { joinType, residual }),
      );
      const hashed = query(
        hashJoin([ordersScan(), usersScan()], byUser, { joinType, residual }),
      );
      expect(looked).toEqual(hashed);
      const matched = looked.filter((row) => userIdOf(row) !== null);
      expect(matched.map(([orderId]) => orderId)).toEqual([11]);
    },
  );
});

describe("a lookup join through an index", () => {
  it.each(["inner", "left"] as const)(
    "seeks an equal key from each left row, duplicates on the left each seeking (%s)",
    (joinType) => {
      // users JOIN users AS other ON other.age = users.age: two users are 30.
      const sameAge = indexScan(
        source(data.users, OTHER_USERS, [0, 1, 2]),
        data.byAge,
        keys([age]),
      );
      const looked = query(lookupJoin(usersScan(), sameAge, { joinType }));
      const hashed = query(
        hashJoin([usersScan(), otherUsers()], [[age], [otherAge]], {
          joinType,
        }),
      );
      expect(looked).toEqual(hashed);
      const thirties = looked.filter(
        ([userId]) => userId === 1 || userId === 4,
      );
      expect(thirties.map((row) => [row[0], userIdOf(row)])).toEqual([
        [1, 1],
        [1, 4],
        [4, 1],
        [4, 4],
      ]);
    },
  );

  it.each(["inner", "left"] as const)(
    "seeks a range bounded by each left row, a NULL bound matching nothing (%s)",
    (joinType) => {
      // users JOIN users AS other ON other.age > users.age, in age order.
      const older = indexScan(
        source(data.users, OTHER_USERS, [0, 1, 2]),
        data.byAge,
        keys([], between(excluding(age), null)),
      );
      const looked = query(lookupJoin(usersScan(), older, { joinType }));
      const byAge = sort(otherUsers(), ascending(otherAge));
      const condition = [compare(otherAge, ">", age)];
      const looped = query(
        nestedLoopJoin(usersScan(), byAge, condition, joinType),
      );
      expect(looked).toEqual(looped);
      // Cid's age is NULL: nothing is older.
      const cid = looked.filter(([userId]) => userId === 3);
      expect(cid).toEqual(
        joinType === "left" ? [[3, "Cid", null, null, null, null]] : [],
      );
    },
  );

  it("seeks an equal prefix and a range on the next part", () => {
    // ON other.age = users.age AND other.name > users.name, on (age, name).
    const sameAgeLater = indexScan(
      source(data.users, OTHER_USERS, [0, 1, 2]),
      data.byAgeName,
      keys([age], between(excluding(name), null)),
    );
    const looked = query(lookupJoin(usersScan(), sameAgeLater));
    const byAgeName = sort(
      otherUsers(),
      ascending(otherAge),
      ascending(otherName),
    );
    const conditions = [
      compare(otherAge, "=", age),
      compare(otherName, ">", name),
    ];
    expect(looked).toEqual(
      query(nestedLoopJoin(usersScan(), byAgeName, conditions)),
    );
    expect(looked).toEqual([[1, "Ann", 30, 4, "dan", 30]]);
  });

  it("seeks a parameter as its affinity converts it", () => {
    // ON other.age = $1 (numeric), given '30' as text.
    const atAge = indexScan(
      source(data.users, OTHER_USERS, [0, 1, 2]),
      data.byAge,
      keys([parameter(0, "numeric")]),
    );
    const looked = query(lookupJoin(ordersScan(), atAge), ["30"]);
    const condition = [compare(otherAge, "=", parameter(0, "numeric"))];
    const looped = query(
      nestedLoopJoin(ordersScan(), otherUsers(), condition),
      ["30"],
    );
    expect(looked).toEqual(looped);
    expect(looked).toHaveLength(10);
  });
});

describe("a lookup join's rows", () => {
  it("come in the left side's order", () => {
    const descending = usersScan("descending");
    const sameAge = indexScan(
      source(data.users, OTHER_USERS, [0, 1, 2]),
      data.byAge,
      keys([age]),
    );
    const leftIds = query(
      lookupJoin(descending, sameAge, { joinType: "left" }),
    ).map(([userId]) => userId);
    expect(leftIds).toEqual([5, 4, 4, 3, 2, 1, 1]);
  });

  it("take one left row at a time under a LIMIT, every row in batches otherwise", () => {
    const counting = new Counting();
    const counted = project(
      COUNTED_ORDERS,
      ordersScan(),
      counting.of(column(ORDERS, 0, "INTEGER")),
      column(ORDERS, 1, "INTEGER"),
    );
    const userOfOrder = rowidList(source(data.users, USERS, [1]), [
      column(COUNTED_ORDERS, 1, "INTEGER"),
    ]);
    const join = lookupJoin(counted, userOfOrder);
    expect(query(limit(join, constant(1)))).toEqual([[10, 1, "Ann"]]);
    expect(counting.calls).toBe(1);
    let checkpoints = 0;
    const rows = data.database.query(join, [], () => {
      checkpoints++;
    });
    expect(rows).toEqual([
      [10, 1, "Ann"],
      [11, 1, "Ann"],
      [12, 2, "bob"],
    ]);
    // The left scan's two pulls and the join's own two.
    expect(checkpoints).toBe(4);
  });
});

describe("a lookup join among subqueries", () => {
  it("keeps the left row apart from the outer rows of a subquery on its left side", () => {
    // Orders whose user exists, by a correlated EXISTS, then that user.
    const userExists = exists(
      filter(otherUsers(), compare(otherId, "=", outer(1, ORDERS, 1))),
      "correlated",
    );
    const left = filter(ordersScan(), userExists);
    const looked = query(lookupJoin(left, userOfEachOrder()));
    const hashed = query(hashJoin([left, usersScan()], [[orderUser], [id]]));
    expect(looked).toEqual(hashed);
    expect(looked.map(([orderId]) => orderId)).toEqual([10, 11, 12]);
  });

  it("seeks on both the outer row and the left row inside a correlated subquery", () => {
    // For each user: how many orders belong to a user of the same age.
    //   (SELECT count(*) FROM orders JOIN users AS other
    //      ON other.age = outer.age AND other.id = orders.user)
    const sameAgeAsOuter = indexScan(
      source(data.users, OTHER_USERS, [0, 1, 2]),
      data.byAge,
      keys([outer(1, USERS, 2)]),
    );
    const pairs = lookupJoin(ordersScan(), sameAgeAsOuter, {
      residual: [compare(otherId, "=", orderUser)],
    });
    const sameAge = filter(
      otherUsers(),
      compare(otherAge, "=", outer(1, USERS, 2)),
    );
    const hashed = hashJoin([ordersScan(), sameAge], [[orderUser], [otherId]]);
    expect(perUser(pairs)).toEqual(perUser(hashed));
    // Users 1 and 4 are 30, and user 1 has two orders; user 2 has one.
    expect(perUser(pairs)).toEqual([
      [1, 2],
      [2, 1],
      [3, 0],
      [4, 2],
      [5, 0],
    ]);
  });
});
