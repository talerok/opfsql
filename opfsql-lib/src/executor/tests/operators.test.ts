import { beforeEach, describe, expect, it } from "vitest";
import { DatatypeMismatchError } from "../../errors.js";
import type { physical } from "../../sql/plan/index.js";
import type { Value } from "../../values/index.js";
import {
  age,
  amount,
  type Fixture,
  fixture,
  id,
  name,
  note,
  ORDERS,
  orderUser,
  USERS,
} from "./fixtures.js";
import {
  aggregateCall,
  arithmetic,
  ascending,
  column,
  compare,
  constant,
  Counting,
  descending,
  distinct,
  filter,
  hashAggregate,
  hashJoin,
  limit,
  nestedLoopJoin,
  NULL,
  parameter,
  project,
  setOperation,
  sort,
  source,
  tableScan,
  topN,
  values,
} from "./plans.js";

let data: Fixture;

beforeEach(async () => {
  data = await fixture();
});

function usersScan(columns = [0, 1, 2]): physical.TableScan {
  return tableScan(source(data.users, USERS, columns));
}

function ordersScan(columns = [0, 1, 2, 3]): physical.TableScan {
  return tableScan(source(data.orders, ORDERS, columns));
}

function query(plan: physical.Plan, parameters: Value[] = []): Value[][] {
  return data.database.query(plan, parameters);
}

/** The first column of each row. */
function firsts(plan: physical.Plan, parameters: Value[] = []): Value[] {
  return query(plan, parameters).map(([first = null]) => first);
}

/** `VALUES (v1), (v2), …` as table 50: one column of `items`. */
function column50(...items: Value[]): physical.Values {
  return values(50, ...items.map((item) => [constant(item)]));
}

/** The users' names, sorted or limited by `wrap`. */
function namesBy(wrap: (scan: physical.Plan) => physical.Plan): Value[] {
  return query(wrap(usersScan())).map(([, nameOfUser = null]) => nameOfUser);
}

/** COUNT(*) and SUM(age) of the child's rows in groups of `groups`. */
function groupsOf(
  groups: physical.Expression[],
  child: physical.Plan,
): physical.HashAggregate {
  return hashAggregate([20, 21], child, groups, [
    aggregateCall("count", []),
    aggregateCall("sum", [age]),
  ]);
}

/** The users' names by age, NULLs last, through a top-n window. */
function topNWindow(count: Value, offset: Value): Value[] {
  const keys = [ascending(age, "last")];
  return namesBy((scan) => topN(scan, keys, constant(count), constant(offset)));
}

/** The users' names through a LIMIT. */
function limitWindow(
  count: physical.Expression | null,
  offset: physical.Expression | null,
): Value[] {
  return namesBy((scan) => limit(scan, count, offset));
}

describe("filter and project", () => {
  it("keep the rows every condition holds for, NULL not being TRUE", () => {
    const adults = filter(
      usersScan(),
      compare(age, ">=", constant(25)),
      compare(name, "<>", constant("dan")),
    );
    expect(firsts(adults)).toEqual([1, 2, 5]);
  });

  it("compute expressions over the child's layout, in their order", () => {
    const plan = project(9, usersScan(), arithmetic(age, "+", constant(1)), id);
    expect(query(plan).slice(0, 3)).toEqual([
      [31, 1],
      [26, 2],
      [null, 3],
    ]);
  });
});

describe("hash join", () => {
  const byUser: [physical.Expression[], physical.Expression[]] = [
    [id],
    [orderUser],
  ];
  const aboveEight = [compare(amount, ">", constant(8))];

  it("joins on equal keys, a pair matching only when its residual is TRUE", () => {
    const sides: [physical.Plan, physical.Plan] = [
      usersScan([0]),
      ordersScan([0, 1, 2]),
    ];
    expect(query(hashJoin(sides, byUser))).toEqual([
      [1, 10, 1, 9.5],
      [1, 11, 1, 20],
      [2, 12, 2, 7],
    ]);
    expect(query(hashJoin(sides, byUser, { residual: aboveEight }))).toEqual([
      [1, 10, 1, 9.5],
      [1, 11, 1, 20],
    ]);
  });

  it("emits a LEFT join's unmatched rows once, with NULLs, a failed residual included", () => {
    const sides: [physical.Plan, physical.Plan] = [
      usersScan([0]),
      ordersScan([0, 1, 2]),
    ];
    const plan = hashJoin(sides, byUser, {
      residual: aboveEight,
      joinType: "left",
    });
    expect(query(plan)).toEqual([
      [1, 10, 1, 9.5],
      [1, 11, 1, 20],
      [2, null, null, null],
      [3, null, null, null],
      [4, null, null, null],
      [5, null, null, null],
    ]);
  });

  it("never matches a NULL key, not even with another NULL", () => {
    const left = usersScan([0, 2]);
    const right = tableScan(source(data.users, 7, [0, 2]));
    const otherAge = { ...age, binding: { tableIndex: 7, columnIndex: 2 } };
    const plan = hashJoin([left, right], [[age], [otherAge]], {
      joinType: "left",
    });
    const pairs = query(plan).map(([one, , other]) => [one, other]);
    // Six pairs of equal ages, and Cid, whose NULL age meets none.
    expect(pairs).toContainEqual([1, 4]);
    expect(pairs).toContainEqual([3, null]);
    expect(pairs).toHaveLength(7);
  });

  it("matches keys as they are: text in an ANY column meets text, not a number", () => {
    const asNumber = hashJoin(
      [ordersScan([0, 3]), usersScan([0])],
      [[note], [id]],
    );
    expect(query(asNumber)).toEqual([]);
    const text = column(50, 0, "TEXT");
    const asText = hashJoin(
      [ordersScan([0, 3]), column50("1")],
      [[note], [text]],
    );
    expect(query(asText)).toEqual([[12, "1", "1"]]);
  });
});

describe("nested loop join", () => {
  it("pairs every row of each side, matched by every condition", () => {
    const conditions = [
      compare(id, "=", orderUser),
      compare(amount, "<", constant(10)),
    ];
    const plan = nestedLoopJoin(
      usersScan([0]),
      ordersScan([0, 1, 2]),
      conditions,
    );
    expect(query(plan).map(([user, order]) => [user, order])).toEqual([
      [1, 10],
      [2, 12],
    ]);
    expect(query(nestedLoopJoin(usersScan([0]), ordersScan([0])))).toHaveLength(
      25,
    );
  });

  it("pads a LEFT join's unmatched rows with NULLs", () => {
    const conditions = [compare(id, "=", orderUser)];
    const plan = nestedLoopJoin(
      usersScan([0]),
      ordersScan([0, 1]),
      conditions,
      "left",
    );
    expect(query(plan).filter(([, order]) => order === null)).toEqual([
      [3, null, null],
      [4, null, null],
      [5, null, null],
    ]);
  });

  it("starts its right side over for each left row", () => {
    const counting = new Counting();
    const right = project(8, column50(1, 2), counting.of(constant(0)));
    expect(query(nestedLoopJoin(column50("a", "b", "c"), right))).toHaveLength(
      6,
    );
    expect(counting.calls).toBe(6);
    expect(counting.compiles).toBe(1);
  });
});

describe("hash aggregate", () => {
  it("makes one group without GROUP BY, even over no rows", () => {
    const none = filter(usersScan(), compare(id, ">", constant(99)));
    expect(query(groupsOf([], none))).toEqual([[0, null]]);
    expect(query(groupsOf([], usersScan()))).toEqual([[5, 126]]);
  });

  it("groups equal values, NULLs together, in the order groups are met", () => {
    data.database.insert(data.users, [[6, "fay", null, null]]);
    expect(query(groupsOf([age], usersScan()))).toEqual([
      [30, 2, 60],
      [25, 1, 25],
      [null, 2, null],
      [41, 1, 41],
    ]);
    const none = filter(usersScan(), compare(id, ">", constant(99)));
    expect(query(groupsOf([age], none))).toEqual([]);
  });

  it("counts DISTINCT arguments once", () => {
    const plan = hashAggregate(
      [20, 21],
      usersScan(),
      [],
      [aggregateCall("count", [age], true)],
    );
    expect(query(plan)).toEqual([[3]]);
  });
});

describe("sort and top-n", () => {
  it("sort stably, NULLs where each key puts them", () => {
    expect(namesBy((scan) => sort(scan, ascending(age)))).toEqual([
      "Cid",
      "bob",
      "Ann",
      "dan",
      "Eve",
    ]);
    expect(namesBy((scan) => sort(scan, descending(age, "first")))).toEqual([
      "Cid",
      "Eve",
      "Ann",
      "dan",
      "bob",
    ]);
    expect(
      namesBy((scan) => sort(scan, ascending(age, "last"), descending(name))),
    ).toEqual(["bob", "dan", "Ann", "Eve", "Cid"]);
  });

  it("take a top-n window: its offset, then its limit, ties in arrival order", () => {
    expect(topNWindow(2, 1)).toEqual(["Ann", "dan"]);
    expect(topNWindow(1, 0)).toEqual(["bob"]);
    expect(topNWindow(-1, 3)).toEqual(["Eve", "Cid"]);
    expect(topNWindow(0, 0)).toEqual([]);
    expect(topNWindow(3, 9)).toEqual([]);
    expect(topNWindow(2, -5)).toEqual(["bob", "Ann"]);
  });
});

describe("limit", () => {
  it("skips its offset, then returns at most its limit", () => {
    expect(limitWindow(constant(2), null)).toEqual(["Ann", "bob"]);
    expect(limitWindow(constant(2), constant(3))).toEqual(["dan", "Eve"]);
    expect(limitWindow(null, constant(4))).toEqual(["Eve"]);
    expect(limitWindow(constant(0), null)).toEqual([]);
    expect(limitWindow(constant(9), constant(9))).toEqual([]);
  });

  it("takes a negative LIMIT as none and a negative OFFSET as 0, as SQLite does", () => {
    expect(limitWindow(constant(-1), constant(3))).toEqual(["dan", "Eve"]);
    expect(limitWindow(constant(1), constant(-2))).toEqual(["Ann"]);
  });

  it("takes integer parameters, text holding one too, and refuses anything else", () => {
    const plan = limit(usersScan(), parameter(0), parameter(1));
    expect(firsts(plan, [1, "2"])).toEqual([3]);
    expect(firsts(plan, [1, 2])).toEqual([3]);
    for (const bad of [1.5, "two", null, true]) {
      expect(() => query(plan, [bad, 0])).toThrow(DatatypeMismatchError);
      expect(() => query(plan, [1, bad])).toThrow(/OFFSET must be an integer/u);
    }
  });

  it("stops pulling its child once it has its rows", () => {
    const counting = new Counting();
    const counted = project(9, usersScan(), counting.of(id));
    expect(firsts(limit(counted, constant(1)))).toEqual([1]);
    expect(counting.calls).toBe(1);
  });
});

describe("hash distinct and set operations", () => {
  it("keep each row once, by the equality ORDER BY uses", () => {
    const ages = project(9, usersScan(), age);
    expect(firsts(distinct(ages))).toEqual([30, 25, null, 41]);
    expect(firsts(distinct(column50(0, -0, "0", null, null)))).toEqual([
      0,
      "0",
      null,
    ]);
  });

  it("UNION ALL, UNION, INTERSECT and EXCEPT", () => {
    const left = column50(1, 2, 2, null);
    const right = values(51, [constant(2)], [constant(3)], [NULL]);
    const run = (operator: physical.SetOperation["operator"]) =>
      firsts(setOperation(operator, 52, left, right));
    expect(run("UNION ALL")).toEqual([1, 2, 2, null, 2, 3, null]);
    expect(run("UNION")).toEqual([1, 2, null, 3]);
    expect(run("INTERSECT")).toEqual([2, null]);
    expect(run("EXCEPT")).toEqual([1]);
  });
});

describe("values", () => {
  it("evaluates its rows, parameters included", () => {
    const plan = values(50, [constant(1), parameter(0)], [NULL, constant("x")]);
    expect(query(plan, ["p"])).toEqual([
      [1, "p"],
      [null, "x"],
    ]);
  });
});
