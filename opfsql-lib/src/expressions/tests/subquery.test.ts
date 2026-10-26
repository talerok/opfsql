import { describe, expect, it } from "vitest";
import { InternalError, SemanticError } from "../../errors.js";
import {
  arithmetic,
  caseWhen,
  column,
  compare,
  compareAll,
  compareAny,
  constant,
  exists,
  FALSE,
  filter,
  inSubquery,
  notExists,
  NULL,
  outer,
  parameter,
  project,
  scalarSubquery,
  values,
} from "./builders.js";
import { c0, evaluate, evaluateEach, fakeExecutor } from "./harness.js";

const none = values(1);
const oneTwo = values(1, [constant(1)], [constant(2)]);
const oneTen = values(1, [constant(1)], [constant(10)]);
const oneNull = values(1, [constant(1)], [NULL]);

/** `SELECT v FROM (VALUES …) AS t1 (v) WHERE v <symbol> t0.c0`, correlated. */
function valuesWhere(symbol: "=" | "<=", ...items: number[]) {
  const rows = items.map((item) => [constant(item)]);
  const condition = compare(column(1, 0, "INTEGER"), symbol, outer(1, 0, 0));
  return filter(values(1, ...rows), condition);
}

describe("subquery results", () => {
  it("EXISTS asks for one row; NOT EXISTS is its negation", () => {
    const executor = fakeExecutor();
    expect(evaluate(exists(oneTwo), { subqueries: executor })).toBe(true);
    expect(executor.limits).toEqual([1]);
    expect(evaluate(exists(none))).toBe(false);
    expect(evaluate(notExists(none))).toBe(true);
  });

  it("a scalar subquery asks for two rows and is its one value, or NULL", () => {
    const executor = fakeExecutor();
    const seven = scalarSubquery(values(1, [constant(7)]));
    expect(evaluate(seven, { subqueries: executor })).toBe(7);
    expect(executor.limits).toEqual([2]);
    expect(evaluate(scalarSubquery(none))).toBeNull();
  });

  it("a scalar subquery with several rows fails", () => {
    expect(() => evaluate(scalarSubquery(oneTwo))).toThrow(SemanticError);
  });

  it("x IN (subquery) is false over no rows, even for a NULL x", () => {
    expect(evaluate(inSubquery(NULL, none))).toBe(false);
    expect(evaluate(inSubquery(NULL, oneTwo))).toBeNull();
    expect(evaluate(inSubquery(c0, oneTen))).toBe(true);
    expect(evaluate(inSubquery(c0, oneNull))).toBeNull();
    expect(evaluate(inSubquery(c0, oneTwo))).toBe(false);
  });

  it("x IN (subquery) answers the same hashed once or run per row", () => {
    const cases = [none, oneTwo, oneTen, oneNull].flatMap((plan) =>
      [c0, NULL].map((needle) => ({ needle, plan })),
    );
    for (const { needle, plan } of cases) {
      const hashed = evaluate(inSubquery(needle, plan));
      expect(evaluate(inSubquery(needle, plan, "correlated"))).toBe(hashed);
    }
  });

  it("x op ALL (subquery) is true over no rows, three-valued otherwise", () => {
    expect(evaluate(compareAll(NULL, ">", none))).toBe(true);
    expect(evaluate(compareAll(c0, ">", oneTwo))).toBe(true);
    expect(evaluate(compareAll(c0, ">", oneNull))).toBeNull();
    expect(
      evaluate(compareAll(c0, ">", values(1, [NULL], [constant(20)]))),
    ).toBe(false);
  });

  it("x op ANY (subquery) holds if the comparison holds for some row", () => {
    expect(evaluate(compareAny(c0, "<", oneTwo))).toBe(false);
    expect(evaluate(compareAny(c0, ">", oneTwo))).toBe(true);
    expect(evaluate(compareAny(c0, "<", oneNull))).toBeNull();
  });

  it("compare with a parameter's affinity, hashed or not", () => {
    const parameters = ["2"];
    const numeric = parameter(0, "numeric");
    expect(evaluate(inSubquery(numeric, oneTwo), { parameters })).toBe(true);
    const correlated = compareAny(numeric, "=", oneTwo, "correlated");
    expect(evaluate(correlated, { parameters })).toBe(true);
    const textual = values(1, [numeric]);
    expect(evaluate(inSubquery(c0, textual), { parameters: ["10"] })).toBe(
      true,
    );
  });

  it("read the run's parameters", () => {
    const echo = scalarSubquery(values(1, [parameter(0)]));
    expect(evaluate(echo, { parameters: [5] })).toBe(5);
  });
});

describe("how often subqueries run", () => {
  it("compiles a correlated subquery once and runs it for each outer row", () => {
    const executor = fakeExecutor();
    const tenfold = scalarSubquery(
      values(1, [arithmetic(outer(1, 0, 0), "*", constant(10))]),
      "correlated",
    );
    expect(
      evaluateEach(tenfold, [[1], [2], [3]], { subqueries: executor }),
    ).toEqual([10, 20, 30]);
    expect(executor.compiled).toHaveLength(1);
    expect(executor.limits).toHaveLength(3);
  });

  it("filters a correlated subquery by the outer row", () => {
    const matches = exists(valuesWhere("=", 1, 3), "correlated");
    expect(evaluateEach(matches, [[1], [2], [3]])).toEqual([true, false, true]);
  });

  it("runs a subquery without outer references once, and keeps its rows", () => {
    const executor = fakeExecutor();
    const seven = scalarSubquery(values(1, [constant(7)]));
    expect(
      evaluateEach(seven, [[1], [2], [3]], { subqueries: executor }),
    ).toEqual([7, 7, 7]);
    expect(executor.limits).toHaveLength(1);
  });

  it("hashes the rows of x IN (subquery without outer references) once", () => {
    const executor = fakeExecutor();
    const isIn = inSubquery(c0, oneTwo);
    expect(
      evaluateEach(isIn, [[1], [3], [null]], { subqueries: executor }),
    ).toEqual([true, false, null]);
    expect(executor.limits).toEqual([undefined]);
  });

  it("never runs a subquery the expression does not reach", () => {
    const executor = fakeExecutor();
    evaluate(caseWhen([[FALSE, scalarSubquery(oneTwo)]]), {
      subqueries: executor,
    });
    expect(executor.compiled).toHaveLength(1);
    expect(executor.limits).toEqual([]);
  });
});

describe("outer references", () => {
  it("read columns two levels out", () => {
    // SELECT (SELECT (SELECT t0.c0 - t1.b) FROM (VALUES (10, 1)) AS t1 (a, b)) FROM t0
    const difference = arithmetic(outer(2, 0, 0), "-", outer(1, 1, 1));
    const innermost = scalarSubquery(values(2, [difference]), "correlated");
    const t1 = values(1, [constant(10), constant(1)]);
    const middle = scalarSubquery(project(3, t1, innermost), "correlated");
    expect(evaluateEach(middle, [[100], [200]])).toEqual([99, 199]);
  });

  it("fail for an outer row narrower than its layout", () => {
    const fourth = scalarSubquery(values(1, [outer(1, 0, 3)]), "correlated");
    expect(() => evaluateEach(fourth, [[1]])).toThrow(InternalError);
  });

  it("are not open to a subquery marked uncorrelated, which runs once for all", () => {
    // SELECT (SELECT (SELECT t1.a) FROM (VALUES (10)) AS t1 (a)) FROM t0,
    // the innermost wrongly marked uncorrelated: with t1's row not pushed,
    // it would read t0's row in its place.
    const misread = scalarSubquery(values(2, [outer(1, 1, 0)]));
    const t1 = values(1, [constant(10)]);
    const middle = scalarSubquery(project(3, t1, misread), "correlated");
    expect(() => evaluateEach(middle, [[100]])).toThrow(
      new InternalError("no outer row 1 levels out (0 deep)"),
    );
  });

  it("read the right row again once a nested subquery has run", () => {
    // SELECT (SELECT (SELECT 1) + t0.c0) FROM t0, the inner one correlated
    const one = scalarSubquery(values(2, [constant(1)]), "correlated");
    const sum = arithmetic(one, "+", outer(1, 0, 0));
    const middle = scalarSubquery(values(1, [sum]), "correlated");
    expect(evaluateEach(middle, [[100], [200]])).toEqual([101, 201]);
  });
});
