import { describe, expect, it } from "vitest";
import {
  asc,
  column,
  compare,
  constant,
  desc,
  filter,
  get,
  inList,
  limit,
  lines,
  order,
  orders,
  output,
  parameter,
  planOf,
  plus,
  project,
  users,
} from "./builders.js";
import { expectSameSequence } from "./same-rows.js";

const u = { tableIndex: 1, table: users };
const o = { tableIndex: 2, table: orders };
const age = column(u, "age");
const name = column(u, "name");
const email = column(u, "email");
const id = column(u, "id");
const adults = filter(get(u), compare(age, ">", constant(18)));

describe("sorts an index delivers", () => {
  it("chooses an index for ORDER BY … LIMIT and keeps the WHERE as its residual", () => {
    const plan = limit(order(adults, asc(name)), constant(10));
    expect(planOf(plan)).toBe(
      lines(
        "limit 10",
        "  index-scan users#1 using users_name_age residual: age > 18",
      ),
    );
  });

  it("drops the sort when the scan's equality prefix leaves the order to the next part", () => {
    const plan = order(
      filter(get(u), compare(name, "=", constant("bob"))),
      asc(age),
    );
    expect(planOf(plan)).toBe(
      "index-scan users#1 using users_name_age (name = 'bob')",
    );
  });

  it("reads backward for DESC NULLS LAST", () => {
    const plan = order(
      filter(get(u), compare(name, "=", constant("bob"))),
      desc(age),
    );
    expect(planOf(plan)).toBe(
      "index-scan users#1 using users_name_age (name = 'bob') descending",
    );
  });

  it("sorts ASC NULLS LAST itself: the index puts NULLs first", () => {
    const plan = order(
      filter(get(u), compare(name, "=", constant("bob"))),
      asc(age, "last"),
    );
    expect(planOf(plan)).toBe(
      lines(
        "sort age ASC NULLS LAST",
        "  index-scan users#1 using users_name_age (name = 'bob')",
      ),
    );
  });

  it("reads a range backward for a descending order", () => {
    const later = filter(get(u), compare(email, ">", constant("e2")));
    const plan = limit(order(later, desc(email)), constant(3));
    expect(planOf(plan)).toBe(
      lines(
        "limit 3",
        "  index-scan users#1 using users_email (email > 'e2') descending",
      ),
    );
    expectSameSequence(plan);
  });

  it("ignores NULL placement on a column that holds no NULL", () => {
    const plan = limit(order(get(u), asc(email, "last")), constant(5));
    expect(planOf(plan)).toBe(
      lines("limit 5", "  index-scan users#1 using users_email"),
    );
  });

  it("reads the table in rowid order for ORDER BY its INTEGER PRIMARY KEY", () => {
    const plan = order(get(u), desc(column(u, "id")));
    expect(planOf(plan)).toBe("table-scan users#1 descending");
    expectSameSequence(plan);
  });

  it("takes rowid order from a rowid range", () => {
    const plan = order(filter(get(u), compare(id, ">", constant(5))), asc(id));
    expect(planOf(plan)).toBe("rowid-lookup users#1 (rowid > 5)");
    expectSameSequence(plan);
  });

  it("takes rowid order from a rowid list, whatever order it lists them in", () => {
    const listed = inList(
      id,
      constant(9),
      constant(2),
      constant(5),
      constant(2),
    );
    const plan = order(filter(get(u), listed), asc(id, "last"));
    expect(planOf(plan)).toBe("rowid-lookup users#1 (rowid IN (9, 2, 5, 2))");
    expectSameSequence(plan);
  });

  it("reads the table backward for a descending rowid order under a LIMIT", () => {
    const range = filter(get(u), compare(id, ">", constant(5)));
    const plan = limit(order(range, desc(id)), constant(3));
    expect(planOf(plan)).toBe(
      lines("limit 3", "  table-scan users#1 descending residual: id > 5"),
    );
    expectSameSequence(plan);
  });

  it("sorts a rowid range descending without a LIMIT: a lookup reads ascending", () => {
    const plan = order(filter(get(u), compare(id, ">", constant(5))), desc(id));
    expect(planOf(plan)).toBe(
      lines("sort id DESC", "  rowid-lookup users#1 (rowid > 5)"),
    );
    expectSameSequence(plan);
  });

  it("sees the order through projects", () => {
    const projected = project(5, adults, name, age);
    const plan = limit(
      order(projected, asc(output(5, 0, "TEXT", "n"))),
      parameter(0),
    );
    expect(planOf(plan)).toBe(
      lines(
        "limit $1",
        "  project#5 name, age",
        "    index-scan users#1 using users_name_age residual: age > 18",
      ),
    );
  });

  it("sorts keys in mixed directions", () => {
    const plan = order(
      filter(get(u), compare(name, "=", constant("bob"))),
      asc(name),
      desc(age),
    );
    expect(planOf(plan)).toBe(
      lines(
        "sort name ASC, age DESC",
        "  index-scan users#1 using users_name_age (name = 'bob')",
      ),
    );
  });
});

describe("sorts that stay", () => {
  it("makes a sort and the limit above it one top-n, without the limit", () => {
    const plan = limit(
      order(get(o), desc(column(o, "total"))),
      constant(5),
      constant(10),
    );
    expect(planOf(plan)).toBe(
      lines("top-n 5 offset 10 by total DESC", "  table-scan orders#2"),
    );
  });

  it("keeps an OFFSET alone as a limit above the sort", () => {
    const plan = limit(
      order(get(o), asc(column(o, "total"))),
      null,
      constant(3),
    );
    expect(planOf(plan)).toBe(
      lines(
        "limit all offset 3",
        "  sort total ASC",
        "    table-scan orders#2",
      ),
    );
  });

  it("takes the limit below projects to the sort", () => {
    const projected = project(
      5,
      order(get(o), asc(column(o, "total"))),
      column(o, "id"),
    );
    expect(planOf(limit(projected, constant(1)))).toBe(
      lines(
        "project#5 id",
        "  top-n 1 by total ASC",
        "    table-scan orders#2",
      ),
    );
  });

  it("sorts by an expression no index holds", () => {
    const plan = order(adults, asc(plus(age, constant(1))));
    expect(planOf(plan)).toBe(
      lines(
        "sort age + 1 ASC",
        "  index-scan users#1 using users_age (age > 18)",
      ),
    );
  });

  it("keeps the sort over a cheaper unordered read when there is no limit", () => {
    const plan = order(
      filter(get(u), compare(email, "=", constant("x"))),
      asc(name),
    );
    expect(planOf(plan)).toBe(
      lines(
        "sort name ASC",
        "  index-scan users#1 using users_email (email = 'x')",
      ),
    );
  });
});
