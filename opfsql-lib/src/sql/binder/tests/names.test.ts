import { describe, expect, it } from "vitest";
import {
  lines,
  plan,
  query,
  schemaOf,
  semanticError,
  subqueries,
} from "./helpers.js";

describe("names", () => {
  it("bind a column to its FROM item's table index and its position", () => {
    expect(plan("SELECT name FROM users WHERE id = 1")).toBe(
      lines(
        "project #1 (name#0.1)",
        "  filter (id#0.0 = 1)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("compare without regard to ASCII case", () => {
    expect(plan("SELECT NAME, Users.AGE FROM USERS")).toBe(
      lines(
        "project #1 (name#0.1, age#0.2)",
        "  get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("give two references to one table two table indexes", () => {
    expect(
      plan(
        "SELECT users.name, u2.name FROM users, users u2 WHERE users.id = u2.id",
      ),
    ).toBe(
      lines(
        "project #2 (name#0.1, name#1.1)",
        "  filter (id#0.0 = id#1.0)",
        "    join inner",
        "      get #0 users (id, name, age, email, data)",
        "      get #1 users (id, name, age, email, data)",
      ),
    );
  });

  it("find an unqualified column in exactly one FROM item", () => {
    expect(semanticError("SELECT id FROM users, orders")).toBe(
      "ambiguous column name: id",
    );
    expect(semanticError("SELECT id FROM users u, users v")).toBe(
      "ambiguous column name: id",
    );
    expect(semanticError("SELECT x FROM users")).toBe("no such column: x");
    expect(semanticError("SELECT u.x FROM users u")).toBe(
      "no such column: u.x",
    );
    expect(semanticError("SELECT users.id FROM users u")).toBe(
      "no such column: users.id",
    );
  });

  it("refuse two FROM items of one name", () => {
    expect(semanticError("SELECT 1 FROM users, users")).toBe(
      "the name users is used twice in FROM",
    );
    expect(semanticError("SELECT 1 FROM users x, orders X")).toBe(
      "the name X is used twice in FROM",
    );
  });

  it("refuse a table that does not exist", () => {
    expect(semanticError("SELECT 1 FROM nowhere")).toBe(
      "no such table: nowhere",
    );
  });

  it("read the names after a column as a JSON path", () => {
    expect(
      plan("SELECT data.address.city, u.data.tags[0], data[1] FROM users u"),
    ).toBe(
      lines(
        "project #1 (data#0.4.address.city, data#0.4.tags[0], data#0.4[1])",
        "  get #0 users (id, name, age, email, data)",
      ),
    );
    expect(semanticError("SELECT name.first FROM users")).toBe(
      "column name is TEXT, not JSON: it has no path .first",
    );
    expect(semanticError("SELECT users.data FROM users u")).toBe(
      "no such column: users.data",
    );
  });

  it("take the innermost query that has a name", () => {
    expect(
      plan(
        "SELECT id FROM users WHERE EXISTS (SELECT id FROM orders WHERE id = 1)",
      ),
    ).toBe(
      lines(
        "project #3 (id#0.0)",
        "  filter (EXISTS subquery(1))",
        "    subquery(1): project #2 (id#1.0)",
        "      filter (id#1.0 = 1)",
        "        get #1 orders (id, user_id, total, status)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });
});

describe("outer references", () => {
  it("count the subqueries in expressions they cross, and correlate each", () => {
    expect(
      plan(
        "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND EXISTS (SELECT 1 FROM items i WHERE i.order_id = o.id AND i.qty > u.age))",
      ),
    ).toBe(
      lines(
        "project #5 (id#0.0)",
        "  filter (EXISTS subquery(1))",
        "    subquery(1, correlated): project #4 (1)",
        "      filter (user_id#1.1 = id#0.0^1) AND (EXISTS subquery(2))",
        "        subquery(2, correlated): project #3 (1)",
        "          filter (order_id#2.0 = id#1.0^1) AND (qty#2.2 > age#0.2^2)",
        "            get #2 items (order_id, sku, qty)",
        "        get #1 orders (id, user_id, total, status)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("correlate a subquery whose nested subquery reads two levels out", () => {
    expect(
      plan(
        "SELECT (SELECT (SELECT u.name FROM items) FROM orders) FROM users u",
      ),
    ).toBe(
      lines(
        "project #5 (subquery(1))",
        "  subquery(1, correlated): project #4 (subquery(2))",
        "    subquery(2, correlated): project #3 (name#0.1^2)",
        "      get #2 items (order_id, sku, qty)",
        "    get #1 orders (id, user_id, total, status)",
        "  get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("leave a subquery without them uncorrelated", () => {
    expect(
      plan("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)"),
    ).toBe(
      lines(
        "project #3 (id#0.0)",
        "  filter (id#0.0 = ANY subquery(1))",
        "    subquery(1): project #2 (user_id#1.1)",
        "      get #1 orders (id, user_id, total, status)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("do not count a subquery in FROM as a level", () => {
    expect(
      plan(
        "SELECT (SELECT s.n FROM (SELECT u.name AS n FROM orders) s) FROM users u",
      ),
    ).toBe(
      lines(
        "project #5 (subquery(1))",
        "  subquery(1, correlated): project #4 (n#3.0)",
        "    project #3 (n#2.0)",
        "      project #2 (name#0.1^1)",
        "        get #1 orders (id, user_id, total, status)",
        "  get #0 users (id, name, age, email, data)",
      ),
    );
  });
});

describe("CTEs read in subqueries", () => {
  it("correlate a subquery whose CTE reads the outer row", () => {
    expect(
      plan(
        "SELECT (WITH c AS (SELECT u.age AS a) SELECT a FROM c) FROM users u",
      ),
    ).toBe(
      lines(
        "project #6 (subquery(1))",
        "  subquery(1, correlated): cte #3 c",
        "    definition: project #2 (age#0.2^1)",
        "      values #1 ()",
        "    body: project #5 (a#4.0)",
        "      cte-scan #4 of #3",
        "  get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("correlate every subquery that reads such a CTE, also through another CTE", () => {
    const nested = lines(
      "project #8 (subquery(1))",
      "  subquery(1, correlated): cte #3 c",
      "    definition: project #2 (age#0.2^1)",
      "      values #1 ()",
      "    body: project #7 (subquery(2))",
      "      subquery(2, correlated): project #6 (a#5.0)",
      "        cte-scan #5 of #3",
      "      get #4 orders (id, user_id, total, status)",
      "  get #0 users (id, name, age, email, data)",
    );
    expect(
      plan(
        "SELECT (WITH c AS (SELECT u.age AS a) SELECT (SELECT a FROM c) FROM orders) FROM users u",
      ),
    ).toBe(nested);
    expect(
      subqueries(
        "SELECT (WITH c AS (SELECT u.age AS a), d AS (SELECT a FROM c) SELECT (SELECT a FROM d)) FROM users u",
      ),
    ).toEqual(["subquery(1, correlated)", "subquery(2, correlated)"]);
  });

  it("correlate a nested subquery reading a CTE of the subquery around it", () => {
    const schema = schemaOf(
      "CREATE TABLE users (id INTEGER PRIMARY KEY)",
      "CREATE TABLE orders (id INTEGER PRIMARY KEY, user INTEGER, amount REAL)",
    );
    expect(
      plan(
        "SELECT id, (WITH mine AS (SELECT amount FROM orders WHERE user = u.id) SELECT count(*) FROM mine WHERE amount > (SELECT avg(amount) FROM mine)) FROM users u",
        schema,
      ),
    ).toBe(
      lines(
        "project #12 (id#0.0, subquery(1))",
        "  subquery(1, correlated): cte #3 mine",
        "    definition: project #2 (amount#1.2)",
        "      filter (user#1.1 = id#0.0^1)",
        "        get #1 orders (id, user, amount)",
        "    body: project #11 (count(*)#10.0)",
        "      aggregate #9 () #10 (count(*))",
        "        filter (amount#4.0 > subquery(2))",
        "          subquery(2, correlated): project #8 (avg(amount)#7.0)",
        "            aggregate #6 () #7 (avg(amount#5.0))",
        "              cte-scan #5 of #3",
        "          cte-scan #4 of #3",
        "  get #0 users (id)",
      ),
    );
  });

  it("leave a subquery uncorrelated when its CTE reads no outer row", () => {
    expect(
      plan(
        "SELECT (WITH c AS (SELECT 1 AS a) SELECT (SELECT a FROM c)) FROM users u",
      ),
    ).toBe(
      lines(
        "project #8 (subquery(1))",
        "  subquery(1): cte #3 c",
        "    definition: project #2 (1)",
        "      values #1 ()",
        "    body: project #7 (subquery(2))",
        "      subquery(2): project #6 (a#5.0)",
        "        cte-scan #5 of #3",
        "      values #4 ()",
        "  get #0 users (id, name, age, email, data)",
      ),
    );
    expect(
      subqueries(
        "WITH c AS (SELECT 1 AS a) SELECT (SELECT a FROM c) FROM users",
      ),
    ).toEqual(["subquery(1)"]);
  });
});

describe("recursive CTEs read in subqueries", () => {
  it("correlate a subquery whose recursive CTE reads the outer row", () => {
    for (const recursive of [
      "WITH RECURSIVE r(n) AS (SELECT u.age UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT max(n) FROM r",
      "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < u.age) SELECT max(n) FROM r",
      "WITH RECURSIVE r(n) AS (WITH c AS (SELECT u.age AS a) SELECT a FROM c UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT max(n) FROM r",
    ]) {
      expect(subqueries(`SELECT (${recursive}) FROM users u`)).toEqual([
        "subquery(1, correlated)",
      ]);
    }
  });

  it("correlate a compound subquery one of whose members reads the outer row", () => {
    expect(
      subqueries(
        "SELECT (SELECT id FROM orders o WHERE o.user_id = 1 UNION SELECT u.age) FROM users u",
      ),
    ).toEqual(["subquery(1, correlated)"]);
  });
});

describe("FROM", () => {
  it("put a subquery's plan under a project of its own", () => {
    expect(
      plan("SELECT s.n FROM (SELECT name AS n FROM users) s WHERE s.n <> 'x'"),
    ).toBe(
      lines(
        "project #3 (n#2.0)",
        "  filter (n#2.0 <> 'x')",
        "    project #2 (n#1.0)",
        "      project #1 (name#0.1)",
        "        get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("do not let a subquery in FROM see the items beside it", () => {
    expect(semanticError("SELECT 1 FROM users u, (SELECT u.id) s")).toBe(
      "no such column: u.id",
    );
  });

  it("read one row of no columns without FROM", () => {
    expect(plan("SELECT 1 + 2 AS three")).toBe(
      lines("project #1 (1 + 2)", "  values #0 ()"),
    );
  });

  it("keep a join's ON conjuncts, and none for a cross join", () => {
    expect(
      plan(
        "SELECT 1 FROM users u JOIN orders o ON o.user_id = u.id AND o.total > 10, items",
      ),
    ).toBe(
      lines(
        "project #3 (1)",
        "  join inner",
        "    join inner on (user_id#1.1 = id#0.0) AND (total#1.2 > 10)",
        "      get #0 users (id, name, age, email, data)",
        "      get #1 orders (id, user_id, total, status)",
        "    get #2 items (order_id, sku, qty)",
      ),
    );
  });

  it("make a RIGHT join a LEFT join with its sides swapped", () => {
    expect(
      plan("SELECT 1 FROM users u RIGHT JOIN orders o ON o.user_id = u.id"),
    ).toBe(
      lines(
        "project #2 (1)",
        "  join left on (user_id#1.1 = id#0.0)",
        "    get #1 orders (id, user_id, total, status)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("join USING columns, which * and unqualified names see once", () => {
    expect(plan("SELECT * FROM users u JOIN orders o USING (id)")).toBe(
      lines(
        "project #2 (id#0.0, name#0.1, age#0.2, email#0.3, data#0.4, user_id#1.1, total#1.2, status#1.3)",
        "  join inner on (id#0.0 = id#1.0)",
        "    get #0 users (id, name, age, email, data)",
        "    get #1 orders (id, user_id, total, status)",
      ),
    );
    expect(
      plan("SELECT id, o.id FROM users LEFT JOIN orders o USING (id)"),
    ).toBe(
      lines(
        "project #2 (id#0.0, id#1.0)",
        "  join left on (id#0.0 = id#1.0)",
        "    get #0 users (id, name, age, email, data)",
        "    get #1 orders (id, user_id, total, status)",
      ),
    );
  });

  it("take a RIGHT join's USING column from its right side", () => {
    expect(plan("SELECT id FROM users RIGHT JOIN orders USING (id)")).toBe(
      lines(
        "project #2 (id#1.0)",
        "  join left on (id#0.0 = id#1.0)",
        "    get #1 orders (id, user_id, total, status)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("need a USING column on each side, once", () => {
    expect(semanticError("SELECT 1 FROM users JOIN orders USING (age)")).toBe(
      "cannot join using column age: it must be in exactly one table on each side",
    );
  });

  it("expand * and t.*, and refuse * without FROM", () => {
    expect(query("SELECT o.*, u.name FROM users u, orders o").columns).toEqual([
      { name: "id", type: "INTEGER" },
      { name: "user_id", type: "INTEGER" },
      { name: "total", type: "REAL" },
      { name: "status", type: "TEXT" },
      { name: "name", type: "TEXT" },
    ]);
    expect(semanticError("SELECT *")).toBe("no tables specified");
    expect(semanticError("SELECT x.* FROM users")).toBe("no such table: x");
  });
});

describe("CTE names", () => {
  it("are seen by the query and by the WITH queries written after them", () => {
    expect(
      plan(
        "WITH a AS (SELECT id FROM users), b AS (SELECT id FROM a) SELECT id FROM b",
      ),
    ).toBe(
      lines(
        "cte #2 a",
        "  definition: project #1 (id#0.0)",
        "    get #0 users (id, name, age, email, data)",
        "  body: cte #5 b",
        "    definition: project #4 (id#3.0)",
        "      cte-scan #3 of #2",
        "    body: project #7 (id#6.0)",
        "      cte-scan #6 of #5",
      ),
    );
  });

  it("hide a table of that name, and end with their query", () => {
    const schema = schemaOf("CREATE TABLE a (x TEXT)");
    expect(plan("WITH a AS (SELECT 1 AS y) SELECT y FROM a", schema)).toBe(
      lines(
        "cte #2 a",
        "  definition: project #1 (1)",
        "    values #0 ()",
        "  body: project #4 (y#3.0)",
        "    cte-scan #3 of #2",
      ),
    );
    expect(
      semanticError(
        "SELECT (WITH a AS (SELECT 1 AS y) SELECT y FROM a), y FROM a",
        schema,
      ),
    ).toBe("no such column: y");
  });

  it("are not used twice in one WITH", () => {
    expect(
      semanticError("WITH a AS (SELECT 1), A AS (SELECT 2) SELECT 1"),
    ).toBe("WITH query name A is used twice");
  });

  it("rename a WITH query's columns, as many as it has", () => {
    expect(
      query("WITH a(x, y) AS (SELECT id, name FROM users) SELECT * FROM a")
        .columns,
    ).toEqual([
      { name: "x", type: "INTEGER" },
      { name: "y", type: "TEXT" },
    ]);
    expect(semanticError("WITH a(x) AS (SELECT 1, 2) SELECT 1")).toBe(
      "WITH query a has 2 columns but 1 names",
    );
  });
});
