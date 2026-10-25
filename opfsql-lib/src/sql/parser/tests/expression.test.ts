import { describe, expect, it } from "vitest";
import { MAX_DEPTH, parse } from "../index.js";
import { StatementType } from "../types.js";
import {
  expression,
  items,
  notSupported,
  render,
  syntaxError,
} from "./helpers.js";

describe("precedence (SQLite's)", () => {
  it.each([
    // The right side of =, LIKE and BETWEEN takes every tighter operator.
    ["name LIKE '%' || $1 || '%'", "(LIKE name (|| (|| '%' $1) '%'))"],
    ["name = 'a' || 'b'", "(= name (|| 'a' 'b'))"],
    ["'a' || 'b' = name", "(= (|| 'a' 'b') name)"],
    ["a BETWEEN 1 + 1 AND 2 * 3", "(BETWEEN a (+ 1 1) (* 2 3))"],
    // The lower bound of BETWEEN takes equality-level operators.
    ["1 BETWEEN 0 = 0 AND 2", "(BETWEEN 1 (= 0 0) 2)"],
    ["x BETWEEN a IS NULL AND b", "(BETWEEN x (IS NULL a) b)"],
    ["x BETWEEN y BETWEEN 0 AND 1 AND 2", "(BETWEEN x (BETWEEN y 0 1) 2)"],
    ["a BETWEEN 1 AND 2 = b", "(= (BETWEEN a 1 2) b)"],
    ["a NOT BETWEEN b AND c", "(NOT (BETWEEN a b c))"],
    // Equality-level operators associate to the left.
    ["a = b IS NULL", "(IS NULL (= a b))"],
    ["a = b = c", "(= (= a b) c)"],
    ["a IS NULL = b", "(= (IS NULL a) b)"],
    ["a = 1 IN (1, 2)", "(IN (= a 1) 1 2)"],
    // Relational binds tighter than equality.
    ["a < b = c > d", "(= (< a b) (> c d))"],
    // || binds tighter than * and +.
    ["'x' || 2 * 3", "(* (|| 'x' 2) 3)"],
    ["1 + 2 || 'a'", "(+ 1 (|| 2 'a'))"],
    ["1 + 2 * 3 - 4 / 5 % 6", "(- (+ 1 (* 2 3)) (% (/ 4 5) 6))"],
    ["a - b - c", "(- (- a b) c)"],
    // NOT binds looser than comparisons, tighter than AND and OR.
    ["NOT a = b", "(NOT (= a b))"],
    ["NOT a AND b", "(AND (NOT a) b)"],
    ["a OR b AND c OR d", "(OR a (AND b c) d)"],
    ["a AND b AND c", "(AND a b c)"],
    ["(a AND b) AND c", "(AND (AND a b) c)"],
    [
      "a = 1 AND b LIKE 'x' OR c IS NOT NULL",
      "(OR (AND (= a 1) (LIKE b 'x')) (IS NOT NULL c))",
    ],
    // Unary minus binds tightest.
    ["-a * b", "(* (NEGATE a) b)"],
    ["- (a + b)", "(NEGATE (+ a b))"],
    ["1 = NOT 0 = 1", "(= 1 (NOT (= 0 1)))"],
  ])("%s", (sql, tree) => {
    expect(expression(sql)).toBe(tree);
  });

  it("parses a long chain without recursion", () => {
    const sql = Array.from({ length: 150 }, (_, index) => `c${String(index)}`);
    expect(expression(sql.join(" AND "))).toMatch(/^\(AND c0 c1 .* c149\)$/);
  });
});

describe("operators", () => {
  it.each([
    ["a ISNULL", "(IS NULL a)"],
    ["a NOTNULL", "(IS NOT NULL a)"],
    ["a NOT NULL", "(IS NOT NULL a)"],
    ["a IS NULL", "(IS NULL a)"],
    ["a IS NOT NULL", "(IS NOT NULL a)"],
    ["a == b", "(= a b)"],
    ["a != b", "(<> a b)"],
    ["a NOT LIKE 'x%'", "(NOT (LIKE a 'x%'))"],
    ["a NOT BETWEEN 1 AND 2", "(NOT (BETWEEN a 1 2))"],
    ["a IN (1, 2, 3)", "(IN a 1 2 3)"],
    ["a NOT IN (1)", "(NOT IN a 1)"],
    ["a IN (SELECT b FROM t)", "(= ANY a <query>)"],
    ["a NOT IN (SELECT b FROM t)", "(NOT (= ANY a <query>))"],
    ["EXISTS (SELECT 1)", "(EXISTS <query>)"],
    ["NOT EXISTS (SELECT 1)", "(NOT_EXISTS <query>)"],
    ["(SELECT 1)", "(SCALAR <query>)"],
    ["+a", "a"],
  ])("%s", (sql, tree) => {
    expect(expression(sql)).toBe(tree);
  });

  it("parses quantified comparisons into the tree", () => {
    expect(expression("a > ALL (SELECT 1)")).toBe("(> ALL a <query>)");
    expect(expression("x = ANY (SELECT 1)")).toBe("(= ANY x <query>)");
    expect(expression("x <> some(SELECT 1)")).toBe("(<> ANY x <query>)");
    expect(expression("x = any")).toBe("(= x any)");
    expect(syntaxError("SELECT x = ANY (1, 2)").message).toBe(
      "expected a subquery, got '1'",
    );
  });

  it.each([
    ["a IS b", "IS with a value other than NULL"],
    ["a IS NOT DISTINCT FROM b", "IS [NOT] DISTINCT FROM"],
    ["a LIKE 'x' ESCAPE '!'", "LIKE … ESCAPE"],
    ["a GLOB 'x'", "GLOB"],
    ["a NOT REGEXP 'x'", "REGEXP"],
    ["a COLLATE nocase", "COLLATE"],
    ["a & b", "the & operator"],
    ["~a", "the ~ operator"],
    ["a -> '$.b'", "the -> operator"],
    ["a IN ()", "an empty IN list"],
    ["(a, b)", "a row value"],
    ["CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"],
    ["count(*) OVER ()", "OVER (window functions)"],
    ["count(*) FILTER (WHERE a)", "FILTER"],
    ["group_concat(v ORDER BY v)", "ORDER BY in a function call"],
    ["a IN t", "IN with a table name"],
    ["[a]", "a name in brackets"],
    ['CAST(a AS "INTEGER")', "a quoted type name"],
    ['CAST(a AS VARYING "CHAR")', "a quoted type name"],
  ])("does not support %s", (sql, feature) => {
    expect(notSupported(`SELECT ${sql}`).feature).toBe(feature);
  });
});

describe("literals", () => {
  it.each([
    ["1", "1"],
    ["1.0", "real(1)"],
    ["1e3", "real(1000)"],
    ["2.5E-1", "real(0.25)"],
    [".5", "real(0.5)"],
    ["0x10", "16"],
    ["0xFFFFFFFFFFFFFFFF", "-1"],
    ["0x00000000000000000001", "1"],
    ["'it''s'", "'it's'"],
    ["x'CAFE'", "x'cafe'"],
    ["NULL", "NULL"],
    ["TRUE", "TRUE"],
    ["false", "FALSE"],
    ["$2", "$2"],
  ])("%s", (sql, value) => {
    expect(expression(sql)).toBe(value);
  });

  it("folds a sign into a number", () => {
    expect(items("-1, - 2.5, +3, -0x10, -x, -(1), - -1")).toEqual([
      "-1",
      "real(-2.5)",
      "3",
      "-16",
      "(NEGATE x)",
      "(NEGATE 1)",
      "(NEGATE -1)",
    ]);
    expect(expression("1 - -1")).toBe("(- 1 -1)");
    expect(expression("-1 * 2")).toBe("(* -1 2)");
    expect(expression("-0")).toBe("0");
  });

  it("keeps integers within the safe range", () => {
    expect(expression("9007199254740991")).toBe("9007199254740991");
    expect(expression("-9007199254740991")).toBe("-9007199254740991");
    for (const literal of [
      "9007199254740992",
      "9007199254740993",
      "-9007199254740992",
      "9".repeat(400),
      "0x20000000000000",
    ]) {
      const error = syntaxError(`SELECT ${literal}`);
      expect(error.message).toMatch(/is outside the safe range/);
      // The span covers the sign, which is part of the literal.
      expect([error.offset, error.length]).toEqual([7, literal.length]);
    }
  });

  it("rejects hex literals longer than 64 bits", () => {
    expect(syntaxError("SELECT 0x10000000000000000").message).toBe(
      "hex literal 0x10000000000000000 has more than 16 digits",
    );
  });
});

describe("names", () => {
  it("reads columns, tables, JSON paths and quoted names", () => {
    expect(
      items(`a, t.a, "order", "a""b", t.data.city, data[0].name, t."key"`),
    ).toEqual([
      "a",
      "t.a",
      "order",
      'a"b',
      "t.data.city",
      "data[0].name",
      "t.key",
    ]);
  });

  it("takes even reserved words as names after a dot", () => {
    expect(expression("data.order.select")).toBe("data.order.select");
  });

  it("uses non-reserved keywords as names", () => {
    expect(items("key, first, last, text, json, index, end, left, if")).toEqual(
      ["key", "first", "last", "text", "json", "index", "end", "left", "if"],
    );
    expect(expression("CASE WHEN end THEN 1 END")).toBe("(CASE (WHEN end 1))");
  });

  it("calls functions named like keywords", () => {
    expect(items("left(s, 2), char(65), replace(s, 'a', 'b')")).toEqual([
      "(left s 2)",
      "(char 65)",
      "(replace s 'a' 'b')",
    ]);
  });

  it("refuses reserved keywords as names unless quoted", () => {
    expect(syntaxError("SELECT order FROM t").message).toBe(
      "expected an expression, got 'order'",
    );
    expect(expression('"default"')).toBe("default");
  });
});

describe("calls and *", () => {
  it("reads calls", () => {
    expect(
      items(
        "count(*), COUNT(DISTINCT a), now(), coalesce(a, b, 1), count(ALL a)",
      ),
    ).toEqual([
      "(count *)",
      "(count DISTINCT a)",
      "(now)",
      "(coalesce a b 1)",
      "(count a)",
    ]);
    expect(syntaxError("SELECT count(ALL)").message).toBe(
      "expected an expression, got ')'",
    );
  });

  it.each([
    "SELECT * + 1 FROM t",
    "SELECT 1 FROM t ORDER BY *",
    "SELECT sum(*) FROM t",
    "SELECT * FROM t WHERE * = 1",
    "SELECT count(t.*) FROM t",
  ])("rejects * outside the select list and count(*): %s", (sql) => {
    expect(syntaxError(sql).code).toBe("SYNTAX_ERROR");
  });

  it("names the rule when * is misplaced", () => {
    const error = syntaxError("SELECT sum(*) FROM t");
    expect(error.message).toBe(
      "'*' is allowed only as a select list item and in count(*)",
    );
    expect([error.offset, error.length]).toEqual([11, 1]);
  });
});

describe("CASE", () => {
  it("keeps the operand of a simple CASE apart from its WHEN values", () => {
    expect(
      expression("CASE f(x) WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END"),
    ).toBe("(CASE (f x) (WHEN 1 'a') (WHEN 2 'b') (ELSE 'c'))");
  });

  it("reads a searched CASE", () => {
    expect(expression("CASE WHEN a > 1 THEN 'big' END")).toBe(
      "(CASE (WHEN (> a 1) 'big'))",
    );
  });

  it("needs a WHEN", () => {
    expect(syntaxError("SELECT CASE x END").message).toBe(
      "expected WHEN, got 'END'",
    );
  });
});

describe("CAST", () => {
  it("keeps the type as written", () => {
    expect(
      items(
        "CAST(a AS INTEGER), CAST(a AS varchar(10)), CAST(a AS DECIMAL(10, 2))",
      ),
    ).toEqual([
      "(CAST a 'INTEGER')",
      "(CAST a 'varchar(10)')",
      "(CAST a 'DECIMAL(10, 2)')",
    ]);
  });

  it("needs a type", () => {
    expect(syntaxError("SELECT CAST(a AS 1)").message).toBe(
      "expected a type name, got '1'",
    );
  });
});

describe("parameters", () => {
  it("numbers ? after the highest number used before it", () => {
    expect(items("?, ?, $5, ?, ?2, ?")).toEqual([
      "$1",
      "$2",
      "$5",
      "$6",
      "$2",
      "$7",
    ]);
  });

  it("numbers each statement on its own", () => {
    const selectLists = parse("SELECT ?; SELECT ?").map((statement) => {
      if (statement.type !== StatementType.SELECT_STATEMENT) return [];
      const { body } = statement;
      if (body.type !== "SELECT_NODE") return [];
      return body.selectList.map((item) => render(item));
    });
    expect(selectLists).toEqual([["$1"], ["$1"]]);
  });

  it("rejects parameter numbers out of range", () => {
    expect(syntaxError("SELECT $0").message).toBe(
      "parameter numbers run from 1 to 32766, got $0",
    );
    expect(syntaxError("SELECT ?40000").offset).toBe(7);
  });
});

function deep(levels: number): string {
  return `SELECT ${"(".repeat(levels)}1${")".repeat(levels)}`;
}

/** The levels `deep` adds around its parentheses: the query and its item. */
const OVERHEAD = 2;

describe("nesting limit", () => {
  it(`allows ${String(MAX_DEPTH)} levels in all and no more`, () => {
    expect(() => parse(deep(MAX_DEPTH - OVERHEAD))).not.toThrow();
    const error = syntaxError(deep(MAX_DEPTH - OVERHEAD + 1));
    expect(error.message).toBe(
      `the statement nests more than ${String(MAX_DEPTH)} levels deep`,
    );
  });

  it.each([
    ["parentheses", deep(5000)],
    ["unary minus", `SELECT ${"- ".repeat(5000)}a`],
    ["NOT", `SELECT ${"NOT ".repeat(5000)}a`],
    ["subqueries", `SELECT ${"(SELECT ".repeat(2000)}1${")".repeat(2000)}`],
    [
      "a chain",
      `SELECT ${Array.from({ length: 5000 }, () => "1").join(" + ")}`,
    ],
    ["joins", `SELECT * FROM t${" JOIN t".repeat(5000)}`],
    [
      "compounds",
      Array.from({ length: 5000 }, () => "SELECT 1").join(" UNION "),
    ],
  ])("stops %s with a syntax error, not a stack overflow", (_name, sql) => {
    expect(syntaxError(sql).message).toMatch(/nests more than/);
  });
});
