import { describe, expect, it } from "vitest";
import {
  formatExpression,
  formatName,
  formatQuery,
  parseExpression,
} from "../index.js";
import { query, render, renderQuery } from "./helpers.js";

/** Expressions of every kind, and the precedence cases of the parser's tests. */
const EXPRESSIONS = [
  "name LIKE '%' || $1 || '%'",
  "'a' || 'b' = name",
  "a BETWEEN 1 + 1 AND 2 * 3",
  "1 BETWEEN 0 = 0 AND 2",
  "x BETWEEN a IS NULL AND b",
  "x BETWEEN y BETWEEN 0 AND 1 AND 2",
  "x BETWEEN 1 AND 2 = b",
  "a NOT BETWEEN b AND c",
  "a = b IS NULL",
  "a = (b IS NULL)",
  "a = b = c",
  "a = (b = c)",
  "a IS NULL = b",
  "a = 1 IN (1, 2)",
  "a < b = c > d",
  "(a = b) < c",
  "'x' || 2 * 3",
  "'x' || (2 * 3)",
  "1 + 2 || 'a'",
  "1 + 2 * 3 - 4 / 5 % 6",
  "a - b - c",
  "a - (b - c)",
  "(a + b) * (c - d)",
  "NOT a = b",
  "NOT a AND b",
  "NOT (a AND b)",
  "NOT NOT a",
  "a OR b AND c OR d",
  "(a OR b) AND (c OR d)",
  "a AND (b AND c)",
  "(a AND b) AND c",
  "-a * b",
  "-(a + b)",
  "-(1)",
  "- -1",
  "-(-a)",
  "1 - -1",
  "-1 * 2",
  "1 = NOT 0 = 1",
  "a = (NOT b)",
  "(NOT EXISTS (SELECT 1)) = TRUE",
  "NOT EXISTS (SELECT 1) = TRUE",
  "a ISNULL",
  "a NOT NULL",
  "a IS NOT NULL",
  "a <> b",
  "a NOT LIKE 'x%'",
  "a IN (1, 2, 3)",
  "a NOT IN (1)",
  "NOT a IN (1)",
  "a IN (SELECT b FROM t)",
  "a NOT IN (SELECT b FROM t)",
  "a + 1 IN (SELECT b FROM t)",
  "a > ALL (SELECT 1)",
  "(a OR b) = ANY (SELECT 1)",
  "x <> SOME (SELECT 1)",
  "EXISTS (SELECT 1)",
  "NOT EXISTS (SELECT 1)",
  "(SELECT 1)",
  "1",
  "1.0",
  "1e3",
  "2.5E-1",
  "-0.5",
  "9e999",
  "-9e999",
  "1e300 * 1e10",
  "0x10",
  "'it''s'",
  "x'CAFE'",
  "NULL",
  "TRUE",
  "FALSE",
  "$2",
  "?",
  "t.a",
  '"order"',
  '"a""b"',
  "t.data.city",
  "data[0].name",
  't."key"',
  "data.order.select",
  "key + first + last + text + json + index + end + left + if",
  '"Été" || "with space" || "x$y"',
  "left(s, 2) || char(65) || replace(s, 'a', 'b')",
  '"like"(a, b)',
  "count(*) + count(DISTINCT a) + now() + coalesce(a, b, 1)",
  "CASE f(x) WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END",
  "CASE WHEN a > 1 THEN 'big' END",
  "CASE WHEN a OR b THEN x AND y ELSE NOT z END",
  "CAST(a AS INTEGER)",
  "CAST(a + 1 AS varchar(10))",
  "CAST(a AS DECIMAL(10, 2))",
];

/** Queries with every clause, join and compound form. */
const QUERIES = [
  "SELECT DISTINCT a, count(*) AS n FROM t WHERE a > 1 GROUP BY a HAVING count(*) > 1 ORDER BY n DESC NULLS LAST, a LIMIT 10 OFFSET 5",
  "SELECT a FROM t ORDER BY a, b DESC, c NULLS FIRST, d ASC NULLS LAST",
  "SELECT a FROM t LIMIT 5, 10",
  "SELECT a FROM t OFFSET 5",
  "SELECT a FROM t LIMIT -1",
  'SELECT *, t.*, "order".* FROM t',
  'SELECT a b, c "d e", f AS key FROM t',
  "SELECT * FROM a x, b JOIN c ON x.i = c.i",
  "SELECT * FROM a CROSS JOIN b ON a.i = b.i",
  "SELECT * FROM a, b USING (i)",
  "SELECT * FROM a JOIN b",
  "SELECT * FROM a INNER JOIN b USING (i, j)",
  "SELECT * FROM a LEFT OUTER JOIN b ON a.i = b.i",
  "SELECT * FROM a RIGHT JOIN b ON TRUE",
  "SELECT * FROM left JOIN right ON left.i = right.i",
  "SELECT * FROM (SELECT 1) AS s, (SELECT 2)",
  'SELECT * FROM "order" o',
  "SELECT 1 UNION ALL SELECT 2 INTERSECT SELECT 3 EXCEPT SELECT DISTINCT 4 ORDER BY 1 LIMIT 2",
  "WITH RECURSIVE a(x) AS (SELECT 1), b AS (SELECT 2 LIMIT 1) SELECT * FROM a UNION SELECT * FROM b",
  "WITH recursive AS (SELECT 1) SELECT * FROM recursive",
  "SELECT (SELECT max(b) FROM u WHERE u.a = t.a) AS m FROM t WHERE EXISTS (SELECT 1 FROM u)",
];

describe("formatExpression", () => {
  it.each(EXPRESSIONS)("prints %s so that it parses back the same", (sql) => {
    const tree = parseExpression(sql);
    const printed = formatExpression(tree);
    const reparsed = parseExpression(printed);
    expect(render(reparsed)).toBe(render(tree));
    expect(formatExpression(reparsed)).toBe(printed);
  });

  it.each([
    ["a+1", "a + 1"],
    ["(a)", "a"],
    ["a - (b - c)", "a - (b - c)"],
    ["(a - b) - c", "a - b - c"],
    ["a = (b = c)", "a = (b = c)"],
    ["a AND (b AND c)", "a AND (b AND c)"],
    ["NOT (a = b)", "NOT a = b"],
    ["a = (NOT b)", "a = (NOT b)"],
    ["-(1)", "-(1)"],
    ["- -1", "-(-1)"],
    ["-(-x)", "-(-x)"],
    ["x / 2.0", "x / 2.0"],
    ["1e3", "1000.0"],
    ["9e999", "9e999"],
    ["x'CAFE'", "X'cafe'"],
    ["'it''s'", "'it''s'"],
    ['"order" + "a""b" + key + Abc_1', '"order" + "a""b" + "key" + Abc_1'],
    ["data[0].name", "data[0].name"],
    ["t.data.order", 't.data."order"'],
    ["COUNT(*)", "count(*)"],
    ["Sum(DISTINCT x)", "sum(DISTINCT x)"],
    ["x IN (SELECT 1)", "x IN (SELECT 1)"],
    ["x = ANY (SELECT 1)", "x IN (SELECT 1)"],
    ["x <> ALL (SELECT 1)", "x <> ALL (SELECT 1)"],
    ["? + ?", "$1 + $2"],
    ["CASE x WHEN 1 THEN 'a' END", "CASE x WHEN 1 THEN 'a' END"],
    ["cast(x as int)", "CAST(x AS int)"],
  ])("prints %s as %s", (sql, printed) => {
    expect(formatExpression(parseExpression(sql))).toBe(printed);
  });
});

describe("formatQuery", () => {
  it.each(QUERIES)("prints %s so that it parses back the same", (sql) => {
    const tree = query(sql);
    const printed = formatQuery(tree);
    expect(renderQuery(query(printed))).toBe(renderQuery(tree));
    expect(formatQuery(query(printed))).toBe(printed);
  });

  it("writes its clauses in their canonical form", () => {
    expect(
      formatQuery(
        query(
          "with c as (select a from t) select a x from c, d where a>1 order by x desc limit 5, 1",
        ),
      ),
    ).toBe(
      "WITH c AS (SELECT a FROM t) SELECT a AS x FROM c CROSS JOIN d WHERE a > 1 ORDER BY x DESC LIMIT 1 OFFSET 5",
    );
  });
});

describe("formatName", () => {
  it("quotes names that are keywords or not plain identifiers", () => {
    expect(
      ["a", "_b1", "x$", "key", "SELECT", "with space", 'say "hi"', "Été"].map(
        (name) => formatName(name),
      ),
    ).toEqual([
      "a",
      "_b1",
      "x$",
      '"key"',
      '"SELECT"',
      '"with space"',
      '"say ""hi"""',
      '"Été"',
    ]);
  });
});

describe("parseExpression", () => {
  it("reads one expression and nothing after it", () => {
    expect(render(parseExpression("lower(name) || '!'"))).toBe(
      "(|| (lower name) '!')",
    );
    expect(() => parseExpression("a b")).toThrow(
      "expected end of input, got 'b'",
    );
  });
});
