import { describe, expect, it } from "vitest";
import { lines, notSupported, plan, query, semanticError } from "./helpers.js";

/** A recursive query r(n) with anchor `SELECT 1` and `step`, then `after`. */
function recursive(step: string, after = ""): string {
  return `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL ${step}${after}) SELECT * FROM r`;
}

describe("set operations", () => {
  it("bind left to right, each under a table index of its own", () => {
    expect(
      plan(
        "SELECT id FROM users INTERSECT SELECT user_id FROM orders EXCEPT SELECT order_id FROM items",
      ),
    ).toBe(
      lines(
        "except #7",
        "  intersect #4",
        "    project #1 (id#0.0)",
        "      get #0 users (id, name, age, email, data)",
        "    project #3 (user_id#2.1)",
        "      get #2 orders (id, user_id, total, status)",
        "  project #6 (order_id#5.0)",
        "    get #5 items (order_id, sku, qty)",
      ),
    );
  });

  it("type each column by the values its sides share, and name it by the left", () => {
    const bound = query("SELECT 1 AS n UNION ALL SELECT 2.5 UNION SELECT NULL");
    expect(bound.columns).toEqual([{ name: "n", type: "REAL" }]);
    expect(bound.plan).toMatchObject({
      kind: "set-operation",
      operator: "UNION",
      types: ["REAL"],
      left: { kind: "set-operation", operator: "UNION ALL", types: ["REAL"] },
    });
  });

  it("need as many columns on each side", () => {
    expect(
      semanticError("SELECT id, name FROM users UNION SELECT id FROM orders"),
    ).toBe("each side of UNION must have as many columns: 2 and 1");
  });
});

describe("WITH RECURSIVE", () => {
  it("is a cte whose definition is a recursive-cte; its step reads the working set", () => {
    expect(
      plan(
        "WITH RECURSIVE t(a, b) AS (SELECT id, name FROM users UNION SELECT t.a, u.name FROM t JOIN users u ON u.id = t.a + 1) SELECT * FROM t",
      ),
    ).toBe(
      lines(
        "cte #7 t",
        "  definition: recursive-cte #2 t #6 union",
        "    anchor: project #1 (id#0.0, name#0.1)",
        "      get #0 users (id, name, age, email, data)",
        "    step: project #5 (a#3.0, name#4.1)",
        "      join inner on (id#4.0 = (a#3.0 + 1))",
        "        cte-scan #3 of #2",
        "        get #4 users (id, name, age, email, data)",
        "  body: project #9 (a#8.0, b#8.1)",
        "    cte-scan #8 of #7",
      ),
    );
  });

  it("widens a column the step makes REAL, and reads it so in the step", () => {
    const bound = query(
      "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 0.5 FROM n WHERE x < 3) SELECT x FROM n",
    );
    expect(bound.columns).toEqual([{ name: "x", type: "REAL" }]);
    expect(bound.plan).toMatchObject({
      kind: "cte",
      definition: {
        kind: "recursive-cte",
        types: ["REAL"],
        step: { child: { child: { kind: "cte-scan", types: ["REAL"] } } },
      },
    });
  });

  it("binds a query that does not name itself as a plain CTE", () => {
    expect(plan("WITH RECURSIVE r AS (SELECT 1 AS n) SELECT n FROM r")).toBe(
      lines(
        "cte #2 r",
        "  definition: project #1 (1)",
        "    values #0 ()",
        "  body: project #4 (n#3.0)",
        "    cte-scan #3 of #2",
      ),
    );
  });

  it("lets anchor and step see its own WITH, wrapped around it", () => {
    expect(
      plan(
        "WITH RECURSIVE r(n) AS (WITH s AS (SELECT 1) SELECT * FROM s UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r",
      ),
    ).toBe(
      lines(
        "cte #2 s",
        "  definition: project #1 (1)",
        "    values #0 ()",
        "  body: cte #9 r",
        "    definition: recursive-cte #5 r #8 union all",
        "      anchor: project #4 (1#3.0)",
        "        cte-scan #3 of #2",
        "      step: project #7 (n#6.0 + 1)",
        "        filter (n#6.0 < 3)",
        "          cte-scan #6 of #5",
        "    body: project #11 (n#10.0)",
        "      cte-scan #10 of #9",
      ),
    );
  });

  it("allows the step's reference on the side of an outer join every row has", () => {
    expect(
      plan(
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT r.n FROM r LEFT JOIN users ON r.n = users.id) SELECT * FROM r",
      ),
    ).toContain("join left on (n#3.0 = id#4.0)");
  });

  it("refuses other shapes", () => {
    expect(semanticError(recursive("SELECT n FROM r, r AS r2"))).toBe(
      "the step of recursive WITH query r must name r exactly once",
    );
    expect(
      semanticError(
        recursive("SELECT 1 FROM users WHERE id IN (SELECT n FROM r)"),
      ),
    ).toBe("recursive reference in a subquery: r");
    expect(semanticError(recursive("SELECT 1 FROM (SELECT n FROM r) s"))).toBe(
      "recursive reference in a subquery: r",
    );
    expect(
      semanticError(
        recursive("SELECT n + 1 FROM r WHERE n < (SELECT max(n) FROM r)"),
      ),
    ).toBe("recursive reference in a subquery: r");
    expect(
      semanticError(
        recursive("SELECT r.n FROM users LEFT JOIN r ON r.n = users.id"),
      ),
    ).toBe(
      "the step of recursive WITH query r names it on the side of an outer join that is filled with NULLs",
    );
    expect(semanticError(recursive("SELECT 'x' FROM r"))).toBe(
      "type mismatch: cannot compare INTEGER with TEXT",
    );
    expect(semanticError(recursive("SELECT n, n FROM r"))).toBe(
      "the anchor and step of recursive WITH query r must have as many columns: 1 and 2",
    );
    expect(
      semanticError(
        "WITH RECURSIVE r(n) AS (SELECT 1 INTERSECT SELECT n FROM r) SELECT * FROM r",
      ),
    ).toBe("recursive WITH query r must have the form anchor UNION [ALL] step");
    expect(
      semanticError(
        "WITH RECURSIVE r(n) AS (SELECT n FROM r UNION SELECT 1) SELECT * FROM r",
      ),
    ).toBe("the anchor of recursive WITH query r names r");
  });

  it("does not support ORDER BY, LIMIT, aggregates or DISTINCT in it", () => {
    expect(notSupported(recursive("SELECT n + 1 FROM r", " ORDER BY 1"))).toBe(
      "ORDER BY in a recursive WITH query",
    );
    expect(notSupported(recursive("SELECT n + 1 FROM r", " LIMIT 5"))).toBe(
      "LIMIT in a recursive WITH query",
    );
    expect(notSupported(recursive("SELECT count(*) FROM r"))).toBe(
      "an aggregate in the step of a recursive WITH query",
    );
    expect(notSupported(recursive("SELECT DISTINCT n FROM r"))).toBe(
      "DISTINCT in the step of a recursive WITH query",
    );
  });
});

describe("subqueries", () => {
  it("bind as values, tests and quantified comparisons", () => {
    const bound = query(
      "SELECT (SELECT max(total) FROM orders), EXISTS (SELECT 1 FROM items), age IN (SELECT qty FROM items) FROM users",
    );
    expect(bound.columns.map((column) => column.type)).toEqual([
      "REAL",
      "BOOLEAN",
      "BOOLEAN",
    ]);
    expect(bound.plan).toMatchObject({
      kind: "project",
      expressions: [
        { kind: "scalar-subquery", isCorrelated: false, type: "REAL" },
        { kind: "exists", isCorrelated: false },
        {
          kind: "quantified-comparison",
          operator: "EQUAL",
          quantifier: "ANY",
        },
      ],
    });
  });

  it("keep their ORDER BY and LIMIT", () => {
    expect(
      plan("SELECT (SELECT name FROM users ORDER BY age DESC LIMIT 1)"),
    ).toBe(
      lines(
        "project #4 (subquery(1))",
        "  subquery(1): limit 1",
        "    project #3 (name#2.0)",
        "      order age#2.1 DESC NULLS LAST",
        "        project #2 (name#1.1, age#1.2)",
        "          get #1 users (id, name, age, email, data)",
        "  values #0 ()",
      ),
    );
  });
});

describe("result columns", () => {
  it("are named by alias, by the column read, or by the expression as SQL prints it", () => {
    expect(
      query(
        "SELECT id AS key, u.name, age+1, COUNT(*), data.city FROM users u GROUP BY id, u.name, age, data",
      ).columns.map((column) => column.name),
    ).toEqual(["key", "name", "age + 1", "count(*)", "data.city"]);
  });
});
