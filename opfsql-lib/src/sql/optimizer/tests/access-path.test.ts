import { describe, expect, it } from "vitest";
import type { logical } from "../../plan/index.js";
import {
  and,
  between,
  call,
  column,
  compare,
  constant,
  exists,
  filter,
  get,
  inList,
  lines,
  NULL,
  or,
  parameter,
  orders,
  planOf,
  users,
} from "./builders.js";
import { expectSameRows } from "./same-rows.js";

const u = { tableIndex: 1, table: users };
const id = column(u, "id");
const rowid = column(u, "rowid");
const age = column(u, "age");
const name = column(u, "name");
const email = column(u, "email");
const tag = column(u, "tag");
/** `$1` compared with a number: text that is exactly a number becomes it. */
const numeric = parameter(0, "INTEGER", "numeric");

function where(...conditions: logical.Expression[]): logical.Plan {
  return filter(get(u), ...conditions);
}

describe("rowid lookups", () => {
  it("looks up a rowid equal to a constant, and drops the condition it enforces", () => {
    const plan = where(compare(id, "=", constant(5)));
    expect(planOf(plan)).toBe("rowid-lookup users#1 (rowid = 5)");
    expectSameRows(plan);
  });

  it("treats ROWID_COLUMN as the INTEGER PRIMARY KEY", () => {
    const plan = where(compare(constant(5), "=", rowid));
    expect(planOf(plan)).toBe("rowid-lookup users#1 (rowid = 5)");
  });

  it("drops a condition with a parameter: the seek uses the value as = does", () => {
    const plan = where(compare(id, "=", numeric));
    expect(planOf(plan)).toBe("rowid-lookup users#1 (rowid = $1)");
    for (const value of [7, "7", "seven", null]) expectSameRows(plan, [value]);
  });

  it("looks up the rowids of an IN list", () => {
    const plan = where(inList(id, constant(1), constant(2), NULL, constant(3)));
    expect(planOf(plan)).toBe(
      "rowid-lookup users#1 (rowid IN (1, 2, NULL, 3))",
    );
    expectSameRows(plan);
  });

  it("reads a range of rowids", () => {
    const plan = where(between(id, constant(1), constant(9)));
    expect(planOf(plan)).toBe(
      "rowid-lookup users#1 (rowid >= 1 AND rowid <= 9)",
    );
    expectSameRows(plan);
  });
});

describe("rowid seeks with values of any kind", () => {
  // Rowid values and bounds are compared with the integer rowids in the
  // total order (physical.ts, RowidSet), as `=` and `<` compare them.
  it.each([
    [compare(id, ">=", constant(1.5)), "rowid-lookup users#1 (rowid >= 1.5)"],
    [compare(id, "<", constant(2.5)), "rowid-lookup users#1 (rowid < 2.5)"],
    [compare(id, "=", constant(1.5)), "rowid-lookup users#1 (rowid = 1.5)"],
    [
      compare(id, "<", constant(1e300)),
      "rowid-lookup users#1 (rowid < 1e+300)",
    ],
  ])("seeks %# as a scan finds it", (condition, printed) => {
    const plan = where(condition);
    expect(planOf(plan)).toBe(printed);
    expectSameRows(plan);
  });

  it("finds every row for id < $1 with text, which sorts after every number", () => {
    const plan = where(compare(id, "<", numeric));
    expect(planOf(plan)).toBe("rowid-lookup users#1 (rowid < $1)");
    for (const value of ["abc", "3", 3.5, null]) expectSameRows(plan, [value]);
  });
});

describe("index scans", () => {
  it("prefers a unique index's whole key to an equality prefix", () => {
    const plan = where(
      compare(age, "=", constant(30)),
      compare(email, "=", constant("e1x1")),
    );
    expect(planOf(plan)).toBe(
      "index-scan users#1 using users_email (email = 'e1x1') residual: age = 30",
    );
    expectSameRows(plan);
  });

  it("prefers the longer equality prefix", () => {
    const plan = where(
      compare(age, "=", constant(30)),
      compare(name, "=", constant("bob")),
    );
    expect(planOf(plan)).toBe(
      "index-scan users#1 using users_name_age (name = 'bob', age = 30)",
    );
    expectSameRows(plan);
  });

  it("follows an equality prefix with a range, exact once bounded below", () => {
    const plan = where(
      compare(name, "=", constant("bob")),
      compare(age, ">", constant(0)),
      compare(age, "<=", constant(65)),
    );
    expect(planOf(plan)).toBe(
      "index-scan users#1 using users_name_age (name = 'bob', age > 0 AND age <= 65)",
    );
    expectSameRows(plan);
  });

  it("keeps a range open below on a column that may hold NULL, which sorts first", () => {
    const plan = where(compare(age, "<", constant(18)));
    expect(planOf(plan)).toBe(
      "index-scan users#1 using users_age (age < 18) residual: age < 18",
    );
    expectSameRows(plan);
  });

  it("drops a range open below on a NOT NULL column", () => {
    const plan = where(compare(email, "<", constant("e2")));
    expect(planOf(plan)).toBe(
      "index-scan users#1 using users_email (email < 'e2')",
    );
    expectSameRows(plan);
  });

  it("lets a parameter bound a range, as exact as a constant", () => {
    const plan = where(compare(age, ">=", numeric));
    expect(planOf(plan)).toBe("index-scan users#1 using users_age (age >= $1)");
    for (const value of [18, "18", "abc", null]) expectSameRows(plan, [value]);
  });

  it("seeks a numeric parameter that stays text, which = compares as text too", () => {
    const plan = where(compare(age, "=", numeric));
    expect(planOf(plan)).toBe("index-scan users#1 using users_age (age = $1)");
    for (const value of ["abc", "5", 5, ""]) expectSameRows(plan, [value]);
  });

  it("matches an expression key to a condition written the same way", () => {
    const plan = where(compare(call("lower", name), "=", constant("bob")));
    expect(planOf(plan)).toBe(
      "index-scan users#1 using users_lower_name (lower(name) = 'bob')",
    );
    expectSameRows(plan);
  });

  it("seeks an ANY column with any value: nothing converts its stored values", () => {
    const plan = where(compare(tag, "=", constant(1)));
    expect(planOf(plan)).toBe("index-scan users#1 using users_tag (tag = 1)");
    expectSameRows(plan);
    const any = where(compare(tag, "=", parameter(0)));
    expect(planOf(any)).toBe("index-scan users#1 using users_tag (tag = $1)");
    for (const value of [5, "5", true, 1.5, null]) expectSameRows(any, [value]);
  });

  it("seeks with NULL, which finds nothing, as = finds nothing", () => {
    const plan = where(compare(age, "=", NULL));
    expect(planOf(plan)).toBe(
      "index-scan users#1 using users_age (age = NULL)",
    );
    expectSameRows(plan);
  });

  it("does not seek with <>", () => {
    const plan = where(compare(age, "<>", constant(3)));
    expect(planOf(plan)).toBe("table-scan users#1 residual: age <> 3");
  });
});

describe("index unions", () => {
  it("reads an OR of indexable branches through each, keeping the OR", () => {
    const plan = where(
      or(
        compare(id, "=", constant(1)),
        compare(email, "=", constant("e2x2")),
        and(
          compare(name, "=", constant("bob")),
          compare(age, ">", constant(3)),
        ),
      ),
    );
    expect(planOf(plan)).toBe(
      "index-union users#1 [rowid = 1; using users_email (email = 'e2x2'); using users_name_age (name = 'bob', age > 3)] residual: id = 1 OR email = 'e2x2' OR name = 'bob' AND age > 3",
    );
    expectSameRows(plan);
  });

  it("reads an IN list on an indexed column as a union of its items", () => {
    const plan = where(inList(age, constant(1), numeric));
    expect(planOf(plan)).toBe(
      "index-union users#1 [using users_age (age = 1); using users_age (age = $1)] residual: age IN (1, $1)",
    );
    for (const value of [30, "30", null]) expectSameRows(plan, [value]);
  });

  it("scans when one branch has no index", () => {
    const plan = where(
      or(compare(id, "=", constant(1)), compare(age, "<>", constant(3))),
    );
    expect(planOf(plan)).toBe(
      "table-scan users#1 residual: id = 1 OR age <> 3",
    );
  });
});

describe("seek values from enclosing queries", () => {
  const o = { tableIndex: 2, table: orders, alias: "o" };
  const p = { tableIndex: 3, table: users, alias: "p" };
  /** The outer users, as a subquery names them. */
  const outer = { ...u, alias: "u" };

  it("seeks a correlated subquery's index on its outer row", () => {
    const inner = filter(
      get(o),
      compare(column(o, "user_id"), "=", column(outer, "id", 1)),
    );
    const plan = where(exists(inner, true));
    expect(planOf(plan)).toBe(
      lines(
        "table-scan users#1 residual: EXISTS (correlated subquery 1)",
        "subquery 1:",
        "  index-scan orders#2 using orders_user (user_id = u.id)",
      ),
    );
    expectSameRows(plan);
  });

  it("seeks on a row two queries out", () => {
    const innermost = filter(
      get(p),
      compare(column(p, "age"), "=", column(outer, "age", 2)),
    );
    const inner = filter(get(o), exists(innermost, true));
    const plan = where(exists(inner, true));
    expect(planOf(plan)).toBe(
      lines(
        "table-scan users#1 residual: EXISTS (correlated subquery 1)",
        "subquery 1:",
        "  table-scan orders#2 residual: EXISTS (correlated subquery 2)",
        "subquery 2:",
        "  index-scan users#3 using users_age (age = u.age)",
      ),
    );
    expectSameRows(plan);
  });

  it("never seeks with a column of the scan's own query", () => {
    const plan = filter(
      get(o),
      compare(column(o, "user_id"), "=", column(o, "id")),
    );
    expect(planOf(plan)).toBe("table-scan orders#2 residual: o.user_id = o.id");
  });
});

describe("parameters never choose the index (rule 5)", () => {
  it("chooses the same index for a parameter as for a constant", () => {
    const withConstant = where(
      compare(age, "=", constant(30)),
      compare(name, ">", constant("a")),
    );
    const withParameter = where(
      compare(age, "=", numeric),
      compare(name, ">", parameter(1, "TEXT", "text")),
    );
    expect(planOf(withConstant)).toBe(
      "index-scan users#1 using users_age (age = 30) residual: name > 'a'",
    );
    expect(planOf(withParameter)).toBe(
      "index-scan users#1 using users_age (age = $1) residual: name > $2",
    );
  });
});
