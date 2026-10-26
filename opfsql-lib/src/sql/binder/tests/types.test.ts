import { describe, expect, it } from "vitest";
import { bind, lines, plan, query, semanticError } from "./helpers.js";

/** The static types of a query's result columns. */
function types(sql: string): string[] {
  return query(sql).columns.map((column) => column.type);
}

function mismatch(left: string, right: string): string {
  return `type mismatch: cannot compare ${left} with ${right}`;
}

describe("static types", () => {
  it("of arithmetic with a REAL: REAL, whatever the other operand", () => {
    expect(
      types(
        "SELECT total + ?, ? * total, data.x / total, total % data.x, NULL - total FROM users, orders",
      ),
    ).toEqual(["REAL", "REAL", "REAL", "REAL", "REAL"]);
  });

  it("of arithmetic: INTEGER for two INTEGERs, REAL with a REAL, ANY when a row decides", () => {
    expect(
      types(
        "SELECT 1 + 1, 1 + 1.5, age / 2, total * age, -age, -total, ? + 1, NULL + 1, data.x + 1, -data.x FROM users, orders",
      ),
    ).toEqual([
      "INTEGER",
      "REAL",
      "INTEGER",
      "REAL",
      "INTEGER",
      "REAL",
      "ANY",
      "INTEGER",
      "ANY",
      "ANY",
    ]);
  });

  it("of values that meet in one column: shared, INTEGER with REAL is REAL", () => {
    expect(
      types(
        "SELECT coalesce(age, total), CASE WHEN age > 1 THEN 1 ELSE 2.5 END, CASE WHEN TRUE THEN NULL ELSE 'a' END, ifnull(age, ?) FROM users, orders",
      ),
    ).toEqual(["REAL", "REAL", "TEXT", "ANY"]);
    expect(
      types("SELECT age FROM users UNION SELECT total FROM orders"),
    ).toEqual(["REAL"]);
  });

  it("of values of different kinds in one column: ANY, decided by the row", () => {
    expect(
      types(
        "SELECT CASE WHEN TRUE THEN 'a' ELSE 2 END, iif(age > 1, 1, 'a'), coalesce(name, 1) FROM users",
      ),
    ).toEqual(["ANY", "ANY", "ANY"]);
    expect(types("SELECT 1 UNION SELECT '1'")).toEqual(["ANY"]);
    expect(
      types("SELECT name FROM users EXCEPT SELECT id FROM orders"),
    ).toEqual(["ANY"]);
    const values = bind("INSERT INTO users (data) VALUES (1), ('a'), (x'00')");
    expect(values).toMatchObject({ source: { types: ["ANY"] } });
  });

  it("of everything else, from the registry and the operators", () => {
    expect(
      types(
        "SELECT 'a' || 1, CAST(age AS TEXT), CAST(1 AS varchar(9)), typeof(age), age IN (1, 2), NULL, data.x, upper(name), name LIKE 'a%' FROM users",
      ),
    ).toEqual([
      "TEXT",
      "TEXT",
      "TEXT",
      "TEXT",
      "BOOLEAN",
      "NULL",
      "JSON",
      "TEXT",
      "BOOLEAN",
    ]);
  });
});

describe("type errors", () => {
  it("refuse comparing typed values of different kinds", () => {
    expect(semanticError("SELECT name > 18 FROM users")).toBe(
      mismatch("TEXT", "INTEGER"),
    );
    expect(semanticError("SELECT TRUE = 1")).toBe(
      mismatch("BOOLEAN", "INTEGER"),
    );
    expect(semanticError("SELECT id = x'00' FROM users")).toBe(
      mismatch("INTEGER", "BLOB"),
    );
    expect(semanticError("SELECT age BETWEEN 'a' AND 'b' FROM users")).toBe(
      mismatch("INTEGER", "TEXT"),
    );
    expect(semanticError("SELECT age IN ('a') FROM users")).toBe(
      mismatch("INTEGER", "TEXT"),
    );
    expect(
      semanticError("SELECT CASE age WHEN 'a' THEN 1 END FROM users"),
    ).toBe(mismatch("INTEGER", "TEXT"));
    expect(
      semanticError(
        "SELECT 1 FROM users WHERE name IN (SELECT id FROM orders)",
      ),
    ).toBe(mismatch("TEXT", "INTEGER"));
  });

  it("compare numbers with numbers, and anything with an untyped or JSON side", () => {
    expect(
      types(
        "SELECT age = total, age = ?, name = NULL, data = 1, data.x = 'a' FROM users, orders",
      ),
    ).toEqual(["BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN"]);
  });

  it("refuse arithmetic and || on what has no number or text", () => {
    expect(semanticError("SELECT 'a' + 1")).toBe(
      "cannot do arithmetic on TEXT",
    );
    expect(semanticError("SELECT -name FROM users")).toBe(
      "cannot do arithmetic on TEXT",
    );
    expect(semanticError("SELECT x'00' || 'a'")).toBe(
      "cannot concatenate a BLOB with ||",
    );
  });

  it("refuse unknown functions, arities, argument types and type names", () => {
    expect(semanticError("SELECT nosuchfn(1)")).toBe(
      "no such function: nosuchfn",
    );
    expect(semanticError("SELECT upper()")).toBe(
      "wrong number of arguments to function upper()",
    );
    expect(semanticError("SELECT abs(name) FROM users")).toBe(
      "abs() does not accept TEXT as argument 1",
    );
    expect(semanticError("SELECT x'00' LIKE 'a'")).toBe(
      "like() does not accept BLOB as argument 2",
    );
    expect(semanticError("SELECT CAST(1 AS NUMBER)")).toBe(
      "unknown type: NUMBER",
    );
  });
});

describe("conditions", () => {
  it("are BOOLEAN, NULL, or a value only a row tells: ANY and JSON", () => {
    expect(plan("SELECT id FROM users WHERE data.active AND ? AND NULL")).toBe(
      lines(
        "project #1 (id#0.0)",
        "  filter data#0.4.active AND $1 AND NULL",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("refuse any other type, in every clause that takes one", () => {
    expect(semanticError("SELECT 1 FROM users WHERE age")).toBe(
      "WHERE must be BOOLEAN, not INTEGER",
    );
    expect(semanticError("SELECT 1 FROM users JOIN orders ON total")).toBe(
      "ON must be BOOLEAN, not REAL",
    );
    expect(semanticError("SELECT 1 FROM users HAVING count(*)")).toBe(
      "HAVING must be BOOLEAN, not INTEGER",
    );
    expect(semanticError("SELECT CASE WHEN age THEN 1 END FROM users")).toBe(
      "a CASE WHEN must be BOOLEAN, not INTEGER",
    );
    expect(semanticError("SELECT NOT age FROM users")).toBe(
      "the operand of NOT must be BOOLEAN, not INTEGER",
    );
    expect(semanticError("SELECT age AND TRUE FROM users")).toBe(
      "an operand of AND must be BOOLEAN, not INTEGER",
    );
    expect(semanticError("SELECT TRUE OR 'x'")).toBe(
      "an operand of OR must be BOOLEAN, not TEXT",
    );
  });
});

describe("operators", () => {
  it("bind negations as NOT around what they negate", () => {
    expect(
      plan(
        "SELECT name NOT LIKE 'b%', id NOT IN (1, 2), email IS NOT NULL, age NOT BETWEEN 1 AND 2, NOT EXISTS (SELECT 1) FROM users",
      ),
    ).toBe(
      lines(
        "project #3 (NOT like('b%', name#0.1), id#0.0 NOT IN (1, 2), email#0.3 IS NOT NULL, age#0.2 NOT BETWEEN 1 AND 2, NOT EXISTS subquery(1))",
        "  subquery(1): project #2 (1)",
        "    values #1 ()",
        "  get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("flatten an AND inside an AND", () => {
    expect(
      plan("SELECT 1 FROM users WHERE age > 1 AND (age < 5 AND id = 2)"),
    ).toBe(
      lines(
        "project #1 (1)",
        "  filter age#0.2 > 1 AND age#0.2 < 5 AND id#0.0 = 2",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });

  it("bind an AND of thousands of operands without recursing on them", () => {
    const conditions = Array.from({ length: 30_000 }, () => "age > 1");
    const bound = query(
      `SELECT 1 FROM users WHERE ${conditions.join(" AND ")}`,
    );
    expect(bound.plan).toMatchObject({
      child: { kind: "filter", conditions: { length: 30_000 } },
    });
  });

  it("need one column of a subquery used as a value or compared", () => {
    expect(semanticError("SELECT (SELECT 1, 2)")).toBe(
      "a scalar subquery must return one column, not 2",
    );
    expect(semanticError("SELECT 1 IN (SELECT 1, 2)")).toBe(
      "a subquery compared with IN, ANY or ALL must return one column, not 2",
    );
    expect(
      plan("SELECT 1 FROM users WHERE age > ALL (SELECT qty FROM items)"),
    ).toBe(
      lines(
        "project #3 (1)",
        "  filter age#0.2 > ALL subquery(1)",
        "    subquery(1): project #2 (qty#1.2)",
        "      get #1 items (order_id, sku, qty)",
        "    get #0 users (id, name, age, email, data)",
      ),
    );
  });
});
