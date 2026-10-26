import { describe, expect, it } from "vitest";
import { lines, notSupported, plan, query, semanticError } from "./helpers.js";

/** The refusal of a column neither grouped nor aggregated. */
function message(column: string): string {
  return `column ${column} must appear in the GROUP BY clause or be used in an aggregate function`;
}

describe("the aggregate node", () => {
  it("binds groups to its group index and calls to its aggregate index", () => {
    expect(
      plan(
        "SELECT age, count(*), sum(total) FROM users u JOIN orders o ON o.user_id = u.id GROUP BY age",
      ),
    ).toBe(
      lines(
        "project #4 (age#2.0, count(*)#3.0, sum(total)#3.1)",
        "  aggregate #2 (age#0.2) #3 (count(*), sum(total#1.2))",
        "    join inner on (user_id#1.1 = id#0.0)",
        "      get #0 users (id, name, age, email, data)",
        "      get #1 orders (id, user_id, total, status)",
      ),
    );
  });

  it("is one group of every row without GROUP BY", () => {
    expect(plan("SELECT count(*), max(age) FROM users")).toBe(
      lines(
        "project #3 (count(*)#2.0, max(age)#2.1)",
        "  aggregate #1 () #2 (count(*), max(age#0.2))",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("computes each call once, however often it is written", () => {
    expect(
      plan(
        "SELECT count(DISTINCT age), count(age), count(DISTINCT age) FROM users",
      ),
    ).toBe(
      lines(
        "project #3 (count(DISTINCT age)#2.0, count(age)#2.1, count(DISTINCT age)#2.0)",
        "  aggregate #1 () #2 (count(DISTINCT age#0.2), count(age#0.2))",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("puts HAVING in a filter above it, and makes a query with HAVING aggregate", () => {
    expect(
      plan(
        "SELECT lower(name), count(*) FROM users GROUP BY lower(name) HAVING count(*) > 1 AND lower(name) <> 'x'",
      ),
    ).toBe(
      lines(
        "project #3 (group1#1.0, count(*)#2.0)",
        "  filter (count(*)#2.0 > 1) AND (group1#1.0 <> 'x')",
        "    aggregate #1 (lower(name#0.1)) #2 (count(*))",
        "      get #0 users (id, name, age, email, data)",
      ),
    );
    expect(plan("SELECT 1 FROM users HAVING count(*) > 2")).toBe(
      lines(
        "project #3 (1)",
        "  filter (count(*)#2.0 > 2)",
        "    aggregate #1 () #2 (count(*))",
        "      get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("aggregates for an aggregate in ORDER BY alone", () => {
    expect(semanticError("SELECT age FROM users ORDER BY count(*)")).toBe(
      "column age must appear in the GROUP BY clause or be used in an aggregate function",
    );
  });
});

describe("expressions above the aggregate", () => {
  it("read a group by matching it structurally", () => {
    expect(plan("SELECT age + 1, count(*) FROM users GROUP BY age")).toBe(
      lines(
        "project #3 (age#1.0 + 1, count(*)#2.0)",
        "  aggregate #1 (age#0.2) #2 (count(*))",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
    expect(plan("SELECT age + 1 FROM users GROUP BY age + 1")).toBe(
      lines(
        "project #3 (group1#1.0)",
        "  aggregate #1 (age#0.2 + 1) #2 ()",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
    expect(plan("SELECT data.city, count(*) FROM users GROUP BY data")).toBe(
      lines(
        "project #3 (data#1.0.city, count(*)#2.0)",
        "  aggregate #1 (data#0.4) #2 (count(*))",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("refuse a column neither grouped nor aggregated, as PostgreSQL does", () => {
    expect(semanticError("SELECT name, count(*) FROM users")).toBe(
      message("name"),
    );
    expect(semanticError("SELECT name, age FROM users GROUP BY name")).toBe(
      message("age"),
    );
    expect(semanticError("SELECT age FROM users GROUP BY age + 1")).toBe(
      message("age"),
    );
    expect(
      semanticError(
        "SELECT count(*) FROM users GROUP BY age HAVING name = 'x'",
      ),
    ).toBe(message("name"));
    expect(semanticError("SELECT * FROM users GROUP BY id")).toBe(
      message("name"),
    );
    expect(
      semanticError("SELECT age FROM users GROUP BY age ORDER BY name"),
    ).toBe(message("name"));
  });

  it("read grouped columns in a correlated subquery, and refuse others there", () => {
    expect(
      plan(
        "SELECT name, (SELECT count(*) FROM orders o WHERE o.user_id = u.id) FROM users u GROUP BY u.id, name",
      ),
    ).toBe(
      lines(
        "project #7 (name#1.1, subquery(1))",
        "  subquery(1, correlated): project #6 (count(*)#5.0)",
        "    aggregate #4 () #5 (count(*))",
        "      filter (user_id#3.1 = id#1.0^1)",
        "        get #3 orders (id, user_id, total, status)",
        "  aggregate #1 (id#0.0, name#0.1) #2 ()",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
    expect(
      semanticError(
        "SELECT (SELECT u.age FROM orders) FROM users u GROUP BY u.id",
      ),
    ).toBe(
      "column age must appear in the GROUP BY clause or be used in an aggregate function",
    );
  });

  it("compare an aggregate with a subquery", () => {
    expect(plan("SELECT sum(age) IN (SELECT 3) FROM users")).toBe(
      lines(
        "project #5 (sum(age)#2.0 = ANY subquery(1))",
        "  subquery(1): project #4 (3)",
        "    values #3 ()",
        "  aggregate #1 () #2 (sum(age#0.2))",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });
});

describe("GROUP BY", () => {
  it("names a select item by position or by alias", () => {
    const grouped = lines(
      "project #3 (age#1.0, count(*)#2.0)",
      "  aggregate #1 (age#0.2) #2 (count(*))",
      "    get #0 users (id, name, age, email, data)",
    );
    expect(plan("SELECT age, count(*) FROM users GROUP BY 1")).toBe(grouped);
    expect(plan("SELECT age AS a, count(*) FROM users GROUP BY a")).toBe(
      grouped,
    );
    expect(semanticError("SELECT age FROM users GROUP BY 2")).toBe(
      "GROUP BY position 2 is not between 1 and 1",
    );
  });

  it("binds a named item once, and the select list reads its group", () => {
    const correlated = lines(
      "project #7 (group1#5.0, count(*)#6.0)",
      "  aggregate #5 (subquery(1)) #6 (count(*))",
      "    subquery(1, correlated): project #4 (max(o.total)#3.0)",
      "      aggregate #2 () #3 (max(total#1.2))",
      "        filter (user_id#1.1 = id#0.0^1)",
      "          get #1 orders (id, user_id, total, status)",
      "    get #0 users (id, name, age, email, data)",
    );
    const item =
      "SELECT (SELECT max(o.total) FROM orders o WHERE o.user_id = u.id) AS m, count(*) FROM users u";
    expect(plan(`${item} GROUP BY 1`)).toBe(correlated);
    expect(plan(`${item} GROUP BY m`)).toBe(correlated);
    expect(query(`${item} GROUP BY m`).columns).toEqual([
      { name: "m", type: "REAL" },
      { name: "count(*)", type: "INTEGER" },
    ]);
  });

  it("computes a volatile item once per group", () => {
    expect(
      plan("SELECT (SELECT random()) AS r, count(*) FROM users GROUP BY 1"),
    ).toBe(
      lines(
        "project #5 (group1#3.0, count(*)#4.0)",
        "  aggregate #3 (subquery(1)) #4 (count(*))",
        "    subquery(1): project #2 (random())",
        "      values #1 ()",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("refuses an alias of two different items, as ORDER BY does", () => {
    expect(
      semanticError("SELECT age AS x, name AS x FROM users GROUP BY x"),
    ).toBe("GROUP BY x is ambiguous");
    expect(
      semanticError("SELECT age AS x, name AS x FROM users ORDER BY x"),
    ).toBe("ORDER BY x is ambiguous");
    expect(
      plan("SELECT age AS x, age AS x, count(*) FROM users GROUP BY x"),
    ).toBe(
      lines(
        "project #3 (age#1.0, age#1.0, count(*)#2.0)",
        "  aggregate #1 (age#0.2) #2 (count(*))",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("prefers a column of the FROM to an alias", () => {
    expect(
      semanticError("SELECT age AS name, count(*) FROM users GROUP BY name"),
    ).toBe(
      "column age must appear in the GROUP BY clause or be used in an aggregate function",
    );
  });

  it("refuses aggregates, also through a position", () => {
    expect(semanticError("SELECT count(*) FROM users GROUP BY count(*)")).toBe(
      "aggregate functions are not allowed in GROUP BY",
    );
    expect(semanticError("SELECT count(*) FROM users GROUP BY 1")).toBe(
      "aggregate functions are not allowed in GROUP BY",
    );
  });
});

describe("aggregate calls", () => {
  it("are not allowed in WHERE, ON or nested in each other", () => {
    expect(semanticError("SELECT 1 FROM users WHERE count(*) > 1")).toBe(
      "aggregate functions are not allowed in WHERE",
    );
    expect(
      semanticError("SELECT 1 FROM users JOIN orders ON sum(total) > 1"),
    ).toBe("aggregate functions are not allowed in JOIN ON");
    expect(semanticError("SELECT sum(count(*)) FROM users")).toBe(
      "aggregate functions are not allowed in the arguments of an aggregate function",
    );
  });

  it("are typed by the registry", () => {
    expect(
      query(
        "SELECT count(*), sum(age), sum(total), avg(age), min(name) FROM users, orders",
      ).columns.map((column) => column.type),
    ).toEqual(["INTEGER", "INTEGER", "REAL", "REAL", "TEXT"]);
    expect(semanticError("SELECT sum(name) FROM users")).toBe(
      "sum() does not accept TEXT as argument 1",
    );
    expect(semanticError("SELECT lower(DISTINCT name) FROM users")).toBe(
      "lower() is not an aggregate function",
    );
  });

  it("belong to the query where they are written", () => {
    expect(
      notSupported("SELECT (SELECT sum(u.age) FROM orders) FROM users u"),
    ).toBe("an aggregate over only an enclosing query's columns");
    expect(
      plan("SELECT (SELECT sum(o.total + u.age) FROM orders o) FROM users u"),
    ).toBe(
      lines(
        "project #5 (subquery(1))",
        "  subquery(1, correlated): project #4 (sum(o.total + u.age)#3.0)",
        "    aggregate #2 () #3 (sum(total#1.2 + age#0.2^1))",
        "      get #1 orders (id, user_id, total, status)",
        "  get #0 users (id, name, age, email, data)",
      ),
    );
  });
});
