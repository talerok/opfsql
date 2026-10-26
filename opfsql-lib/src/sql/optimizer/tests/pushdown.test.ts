import { describe, expect, it } from "vitest";
import {
  aggregate,
  aggregateCall,
  column,
  compare,
  constant,
  cte,
  cteScan,
  distinct,
  exists,
  filter,
  get,
  join,
  limit,
  lines,
  nullCheck,
  orders,
  output,
  planOf,
  project,
  random,
  setOperation,
  users,
  values,
} from "./builders.js";
import { expectSameRows } from "./same-rows.js";

const u = { tableIndex: 1, table: users, alias: "u" };
const o = { tableIndex: 2, table: orders, alias: "o" };
const age = column(u, "age");
const total = column(o, "total");
const userOf = compare(column(o, "user_id"), "=", column(u, "id"));

describe("through projects", () => {
  it("substitutes the projected expression and reaches the scan", () => {
    const projected = project(5, get(u), column(u, "name"), age);
    const plan = filter(
      projected,
      compare(output(5, 1, "INTEGER", "a"), ">", constant(18)),
    );
    expect(planOf(plan)).toBe(
      lines(
        "project#5 u.name, u.age",
        "  index-scan users#1 using users_age (age > 18)",
      ),
    );
  });

  it("stays above a project whose expression is volatile", () => {
    const projected = project(5, get(u), random());
    const plan = filter(
      projected,
      compare(output(5, 0, "INTEGER", "r"), ">", constant(0)),
    );
    expect(planOf(plan)).toBe(
      lines("filter r > 0", "  project#5 random()", "    table-scan users#1"),
    );
  });
});

describe("into joins", () => {
  it("sends each side its own conjuncts, and the rest into the inner join", () => {
    const plan = filter(
      join("inner", get(u), get(o)),
      userOf,
      compare(total, ">", constant(5)),
      compare(age, "<", total),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join inner on u.id = o.user_id residual: u.age < o.total",
        "  table-scan users#1",
        "  table-scan orders#2 residual: o.total > 5",
      ),
    );
  });

  it("sends a LEFT join's WHERE into its left side, and its ON into its right", () => {
    const plan = filter(
      join(
        "left",
        get(u),
        get(o),
        userOf,
        compare(column(o, "status"), "=", constant("paid")),
      ),
      compare(column(u, "name"), "=", constant("bob")),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join left on u.id = o.user_id",
        "  index-scan users#1 using users_name_age (name = 'bob')",
        "  table-scan orders#2 residual: o.status = 'paid'",
      ),
    );
  });

  it("never sends a WHERE conjunct into a LEFT join's right side", () => {
    // `o.total IS NULL` keeps users without orders: below the join it would
    // see no NULLs and keep the users whose orders have no total instead.
    const plan = filter(join("left", get(u), get(o), userOf), nullCheck(total));
    expect(planOf(plan)).toBe(
      lines(
        "filter o.total IS NULL",
        "  hash-join left on u.id = o.user_id",
        "    table-scan users#1",
        "    table-scan orders#2",
      ),
    );
  });

  it("keeps a LEFT join's ON conjunct on its left side in the join", () => {
    const plan = join(
      "left",
      get(u),
      get(o),
      userOf,
      compare(age, ">", constant(18)),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join left on u.id = o.user_id residual: u.age > 18",
        "  table-scan users#1",
        "  table-scan orders#2",
      ),
    );
  });
});

describe("below aggregates", () => {
  const grouped = aggregate(
    { groupIndex: 3, aggregateIndex: 4 },
    get(u),
    [age],
    aggregateCall("count"),
  );
  const group = output(3, 0, "INTEGER", "age");
  const count = output(4, 0, "INTEGER", "count");

  it("moves a condition on group keys below, and keeps one on aggregates above", () => {
    const plan = filter(
      grouped,
      compare(group, ">", constant(18)),
      compare(count, ">", constant(1)),
    );
    expect(planOf(plan)).toBe(
      lines(
        "filter count > 1",
        "  hash-aggregate#3#4 group by u.age compute count(*)",
        "    index-scan users#1 using users_age (age > 18)",
      ),
    );
  });

  it("keeps everything above an aggregate without groups, which makes a row of nothing", () => {
    const counted = aggregate(
      { groupIndex: 3, aggregateIndex: 4 },
      get(u),
      [],
      aggregateCall("count"),
    );
    const plan = filter(
      counted,
      compare(constant(1), "=", output(4, 0, "INTEGER", "count")),
    );
    expect(planOf(plan)).toBe(
      lines(
        "filter 1 = count",
        "  hash-aggregate#3#4 compute count(*)",
        "    table-scan users#1",
      ),
    );
  });
});

describe("into set operations", () => {
  const left = values(6, [constant(1)], [constant(2)]);
  const right = project(7, get(u), age);
  const result = output(8, 0, "INTEGER", "x");

  it("goes into both sides of a UNION ALL, over each side's columns", () => {
    const plan = filter(
      setOperation("UNION ALL", 8, left, right),
      compare(result, ">", constant(1)),
    );
    expect(planOf(plan)).toBe(
      lines(
        "set-operation#8 UNION ALL",
        "  filter x > 1",
        "    values#6 (1), (2)",
        "  project#7 u.age",
        "    index-scan users#1 using users_age (age > 1)",
      ),
    );
  });

  it("stays above an INTERSECT", () => {
    const plan = filter(
      setOperation("INTERSECT", 8, left, right),
      compare(result, ">", constant(1)),
    );
    expect(planOf(plan)).toBe(
      lines(
        "filter x > 1",
        "  set-operation#8 INTERSECT",
        "    values#6 (1), (2)",
        "    project#7 u.age",
        "      table-scan users#1",
      ),
    );
  });

  it("goes into both sides of a UNION, whose duplicates pass or fail together", () => {
    const plan = filter(
      setOperation("UNION", 8, left, right),
      compare(result, "<", constant(2)),
    );
    expect(planOf(plan)).toBe(
      lines(
        "set-operation#8 UNION",
        "  filter x < 2",
        "    values#6 (1), (2)",
        "  project#7 u.age",
        "    index-scan users#1 using users_age (age < 2) residual: u.age < 2",
      ),
    );
    expectSameRows(plan);
  });

  it("stays above a union whose sides have other types than its result", () => {
    // TEXT on one side and INTEGER on the other: the result is ANY.
    const union = setOperation("UNION", 8, values(6, [constant("a")]), right);
    const mixed = output(8, 0, "ANY", "x");
    const plan = filter(union, compare(mixed, "=", constant(1)));
    expect(planOf(plan)).toBe(
      lines(
        "filter x = 1",
        "  set-operation#8 UNION",
        "    values#6 ('a')",
        "    project#7 u.age",
        "      table-scan users#1",
      ),
    );
  });
});

describe("into CTEs", () => {
  it("goes into a CTE's body, and its definition gets its own", () => {
    const definition = project(
      7,
      filter(get(u), compare(age, ">", constant(1))),
      age,
    );
    const read = cteScan(1, 9, definition);
    const plan = filter(
      cte(1, "adults", definition, read),
      compare(output(9, 0, "INTEGER", "a.age"), "<", constant(65)),
    );
    expect(planOf(plan)).toBe(
      lines(
        "cte#1 adults",
        "  project#7 u.age",
        "    index-scan users#1 using users_age (age > 1)",
        "  filter a.age < 65",
        "    cte-scan#9 of cte#1",
      ),
    );
    expectSameRows(plan);
  });
});

describe("conditions that stay", () => {
  it("never moves a volatile condition past a join", () => {
    const plan = filter(
      join("inner", get(u), get(o), userOf),
      compare(random(), "<", constant(0)),
    );
    expect(planOf(plan)).toBe(
      lines(
        "filter random() < 0",
        "  hash-join inner on u.id = o.user_id",
        "    table-scan users#1",
        "    table-scan orders#2",
      ),
    );
  });

  it("never moves a correlated subquery", () => {
    const inner = filter(
      get({ tableIndex: 9, table: orders }),
      compare(
        column({ tableIndex: 9, table: orders }, "user_id"),
        "=",
        column(u, "id", 1),
      ),
    );
    const plan = filter(
      join("inner", get(u), get(o), userOf),
      exists(inner, true),
    );
    expect(planOf(plan)).toBe(
      lines(
        "filter EXISTS (correlated subquery 1)",
        "  hash-join inner on u.id = o.user_id",
        "    table-scan users#1",
        "    table-scan orders#2",
        "subquery 1:",
        "  index-scan orders#9 using orders_user (user_id = u.id)",
      ),
    );
  });

  it("stays above a limit", () => {
    const plan = filter(
      limit(get(u), constant(5)),
      compare(age, ">", constant(1)),
    );
    expect(planOf(plan)).toBe(
      lines("filter u.age > 1", "  limit 5", "    table-scan users#1"),
    );
  });

  it("passes through DISTINCT", () => {
    const plan = filter(distinct(get(u)), compare(age, ">", constant(1)));
    expect(planOf(plan)).toBe(
      lines("hash-distinct", "  index-scan users#1 using users_age (age > 1)"),
    );
  });
});
