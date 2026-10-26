import { describe, expect, it } from "vitest";
import {
  column,
  compare,
  constant,
  exists,
  filter,
  get,
  join,
  lines,
  orders,
  parameter,
  planOf,
  plus,
  random,
  users,
} from "./builders.js";
import { expectSameRows } from "./same-rows.js";
import { sqlPlanOf } from "./sql.js";

const u = { tableIndex: 1, table: users, alias: "u" };
const o = { tableIndex: 2, table: orders, alias: "o" };
const id = column(u, "id");
const userId = column(o, "user_id");

// Joins of tables that need a full read: no condition of their own picks
// out a few rows, so they meet in a hash join (or a nested loop), unless a
// condition lets one side be found by the other's key.
describe("join planning", () => {
  it("hashes on an equality with one side's columns on each side, either way round", () => {
    expect(
      planOf(join("inner", get(u), get(o), compare(userId, "=", id))),
    ).toBe(
      lines(
        "hash-join inner on u.id = o.user_id",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("keeps several keys, in the order written", () => {
    const plan = join(
      "inner",
      get(u),
      get(o),
      compare(id, "=", userId),
      compare(column(u, "age"), "=", column(o, "id")),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join inner on u.id = o.user_id, u.age = o.id",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("keeps everything that is not a key as the residual, never dropping it", () => {
    const plan = join(
      "inner",
      get(u),
      get(o),
      compare(id, "=", userId),
      compare(column(o, "total"), ">", column(u, "age")),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join inner on u.id = o.user_id residual: o.total > u.age",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("keeps a LEFT join's residual in the join, deciding matches", () => {
    const plan = join(
      "left",
      get(u),
      get(o),
      compare(id, "=", userId),
      compare(column(o, "total"), ">", column(u, "age")),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join left on u.id = o.user_id residual: o.total > u.age",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("loops when no condition is an equality key or a seek", () => {
    const total = column(o, "total");
    const plan = join("inner", get(u), get(o), compare(id, "<", total));
    expect(planOf(plan)).toBe(
      lines(
        "nested-loop-join inner on u.id < o.total",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("seeks a range bounded by the left row rather than loop over every pair", () => {
    const plan = join("inner", get(u), get(o), compare(id, "<", userId));
    expect(planOf(plan)).toBe(
      lines(
        "lookup-join inner",
        "  table-scan users#1",
        "  index-scan orders#2 using orders_user (user_id > u.id)",
      ),
    );
    expectSameRows(plan);
  });

  it("does not take an equality reading both sides on one side as a key", () => {
    const plan = join(
      "inner",
      get(u),
      get(o),
      compare(plus(id, userId), "=", constant(3)),
    );
    expect(planOf(plan)).toBe(
      lines(
        "nested-loop-join inner on u.id + o.user_id = 3",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("does not take a volatile equality as a key", () => {
    const plan = join(
      "inner",
      get(u),
      get(o),
      compare(plus(id, random()), "=", userId),
    );
    expect(planOf(plan)).toBe(
      lines(
        "nested-loop-join inner on u.id + random() = o.user_id",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });

  it("pairs every row of a cross join", () => {
    expect(planOf(join("inner", get(u), get(o)))).toBe(
      lines(
        "nested-loop-join inner",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });
});

describe("lookup joins", () => {
  const p = { tableIndex: 3, table: users, alias: "p" };
  const theOrder = compare(column(o, "id"), "=", parameter(0, "INTEGER"));
  const theUser = compare(id, "=", parameter(0, "INTEGER"));

  it("let the side found by its rowid lead, and find the other's row by rowid", () => {
    const plan = filter(
      join("inner", get(u), get(o), compare(id, "=", userId)),
      theOrder,
    );
    expect(planOf(plan)).toBe(
      lines(
        "lookup-join inner",
        "  rowid-lookup orders#2 (rowid = $1)",
        "  rowid-lookup users#1 (rowid = o.user_id)",
      ),
    );
    expectSameRows(plan, [3]);
  });

  it("find the other side's rows through an index on its key", () => {
    const plan = filter(
      join("inner", get(o), get(u), compare(userId, "=", id)),
      theUser,
    );
    expect(planOf(plan)).toBe(
      lines(
        "lookup-join inner",
        "  rowid-lookup users#1 (rowid = $1)",
        "  index-scan orders#2 using orders_user (user_id = u.id)",
      ),
    );
    expectSameRows(plan, [5]);
  });

  it("keep every condition the seek does not enforce exactly as the join's residual", () => {
    const plan = filter(
      join(
        "inner",
        get(u),
        get(o),
        compare(userId, "=", id),
        compare(column(o, "total"), ">", column(u, "age")),
      ),
      theUser,
    );
    expect(planOf(plan)).toBe(
      lines(
        "lookup-join inner residual: o.total > u.age",
        "  rowid-lookup users#1 (rowid = $1)",
        "  index-scan orders#2 using orders_user (user_id = u.id)",
      ),
    );
    expectSameRows(plan, [5]);
  });

  it("keep a LEFT join's sides, its right side's own conditions on its scan", () => {
    const leftJoin = join(
      "left",
      get(o),
      get(u),
      compare(id, "=", userId),
      compare(column(u, "age"), ">", constant(18)),
    );
    expect(planOf(filter(leftJoin, theOrder))).toBe(
      lines(
        "lookup-join left",
        "  rowid-lookup orders#2 (rowid = $1)",
        "  rowid-lookup users#1 (rowid = o.user_id) residual: u.age > 18",
      ),
    );
    expectSameRows(filter(leftJoin, theOrder), [3]);
    // Every order read, users needing a full read too: a hash join.
    expect(planOf(leftJoin)).toBe(
      lines(
        "hash-join left on o.user_id = u.id",
        "  table-scan orders#2",
        "  index-scan users#1 using users_age (age > 18)",
      ),
    );
    expectSameRows(leftJoin);
  });

  it("start a chain with its most selective table, each next joined the cheapest way", () => {
    const plan = filter(
      join(
        "inner",
        join("inner", get(u), get(o), compare(id, "=", userId)),
        get(p),
        compare(column(p, "id"), "=", userId),
      ),
      theOrder,
    );
    expect(planOf(plan)).toBe(
      lines(
        "lookup-join inner",
        "  lookup-join inner",
        "    rowid-lookup orders#2 (rowid = $1)",
        "    rowid-lookup users#1 (rowid = o.user_id)",
        "  rowid-lookup users#3 (rowid = o.user_id)",
      ),
    );
    expectSameRows(plan, [3]);
  });

  it("hash the side estimated smaller when both need a full read", () => {
    // users by a range of age, a share of the table; orders, all of it.
    const plan = join(
      "inner",
      filter(get(u), compare(column(u, "age"), ">", constant(18))),
      get(o),
      compare(column(o, "status"), "=", column(u, "email")),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join inner on o.status = u.email",
        "  table-scan orders#2",
        "  index-scan users#1 using users_age (age > 18)",
      ),
    );
    expectSameRows(plan);
  });

  it("do not move a condition with a correlated subquery to another join", () => {
    const inner = { tableIndex: 9, table: orders, alias: "i" };
    const pinned = exists(
      filter(
        get(inner),
        compare(column(inner, "user_id"), "=", column(p, "id", 1)),
      ),
      true,
    );
    const plan = filter(
      join(
        "inner",
        join("inner", get(u), get(o), compare(id, "=", userId)),
        get(p),
        compare(column(p, "id"), "=", userId),
        pinned,
      ),
      theOrder,
    );
    // The pinned join keeps its two sides, u ⋈ o planned on its own.
    expect(planOf(plan)).toMatch(
      /^lookup-join inner residual: EXISTS \(correlated subquery 1\)\n {2}lookup-join inner\n/u,
    );
    expectSameRows(plan, [3]);
  });
});

describe("the bench's joins, from SQL", () => {
  it("find an order's user by rowid", () => {
    expect(
      sqlPlanOf(
        "SELECT o.id, o.total, u.name FROM orders o JOIN users u ON u.id = o.user_id WHERE o.id = $1",
      ),
    ).toBe(
      lines(
        "project#2 id, total, name",
        "  lookup-join inner",
        "    rowid-lookup orders#0 (rowid = $1)",
        "    rowid-lookup users#1 (rowid = user_id)",
      ),
    );
  });

  it("find a user's orders through orders_user, the user leading", () => {
    expect(
      sqlPlanOf(
        "SELECT o.id, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = $1",
      ),
    ).toBe(
      lines(
        "project#2 id, total",
        "  lookup-join inner",
        "    rowid-lookup users#0 (rowid = $1)",
        "    index-scan orders#1 using orders_user (user_id = id)",
      ),
    );
  });
});
