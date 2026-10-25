// How the binder takes the constructs the rebuilt parser reads: it binds
// them as before, or refuses them with NOT_SUPPORTED or SEMANTIC_ERROR,
// never with a wrong plan.

import { beforeEach, describe, expect, it } from "vitest";
import { NotSupportedError, SemanticError } from "../../../errors.js";
import type {
  BoundCaseExpression,
  BoundSubqueryExpression,
  LogicalCreateTable,
  LogicalFilter,
  LogicalOperator,
  LogicalOrderBy,
  LogicalProjection,
} from "../types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../types.js";
import { createTestContext } from "./test_helpers.js";

let bind: ReturnType<typeof createTestContext>["bind"];

beforeEach(() => {
  bind = createTestContext().bind;
});

function columns(sql: string): [string, string, unknown][] {
  const plan = bind(sql) as LogicalCreateTable;
  return plan.schema.columns.map((column) => [
    column.name,
    column.type,
    column.defaultValue,
  ]);
}

function find(
  plan: LogicalOperator,
  type: LogicalOperatorType,
): LogicalOperator | undefined {
  if (plan.type === type) return plan;
  for (const child of plan.children) {
    const found = find(child, type);
    if (found !== undefined) return found;
  }
  return undefined;
}

describe("compound queries", () => {
  it("does not support INTERSECT and EXCEPT", () => {
    for (const operator of ["INTERSECT", "EXCEPT"]) {
      expect(() =>
        bind(`SELECT id FROM users ${operator} SELECT id FROM orders`),
      ).toThrow(new NotSupportedError(operator));
    }
  });

  it("does not take INTERSECT for the UNION of a recursive CTE", () => {
    expect(() =>
      bind(
        "WITH RECURSIVE r(n) AS (SELECT 1 INTERSECT SELECT n + 1 FROM r) SELECT n FROM r",
      ),
    ).toThrow("must use UNION or UNION ALL");
  });

  it("lets the anchor of a recursive CTE see a WITH in its body", () => {
    const plan = bind(
      "WITH RECURSIVE r(n) AS (WITH s AS (SELECT 1) SELECT * FROM s UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r",
    );
    expect(find(plan, LogicalOperatorType.LOGICAL_RECURSIVE_CTE)).toBeDefined();
  });

  it("refuses a WITH name used twice", () => {
    expect(() =>
      bind("WITH a AS (SELECT 1), A AS (SELECT 2) SELECT * FROM a"),
    ).toThrow(SemanticError);
  });

  it("lets both sides of a compound query see its WITH", () => {
    const plan = bind(
      "WITH c AS (SELECT 1 AS x) SELECT x FROM c UNION ALL SELECT x FROM c",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
  });
});

describe("expressions", () => {
  it("binds a simple CASE as one comparison per WHEN, as before", () => {
    const plan = bind(
      "SELECT CASE age WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM users",
    ) as LogicalProjection;
    const expression = plan.expressions[0] as BoundCaseExpression;
    expect(
      expression.caseChecks.map((check) => check.when.expressionClass),
    ).toEqual([
      BoundExpressionClass.BOUND_COMPARISON,
      BoundExpressionClass.BOUND_COMPARISON,
    ]);
  });

  it("finds an aggregate in a CASE operand", () => {
    const plan = bind(
      "SELECT CASE count(*) WHEN 0 THEN 'none' ELSE 'some' END FROM users",
    );
    expect(
      find(plan, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY),
    ).toBeDefined();
  });

  it("binds ALL and ANY comparisons", () => {
    const plan = bind(
      "SELECT id FROM users WHERE id > ALL (SELECT user_id FROM orders)",
    );
    const filter = find(
      plan,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    const subquery = filter.expressions[0] as BoundSubqueryExpression;
    expect([subquery.subqueryType, subquery.comparisonType]).toEqual([
      "ALL",
      "GREATER",
    ]);
  });

  it("defaults the NULL order: last ascending, first descending", () => {
    const plan = bind(
      "SELECT id FROM users ORDER BY name, age DESC, id NULLS FIRST",
    );
    const order = find(
      plan,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(order.orders.map((item) => item.nullOrder)).toEqual([
      "NULLS_LAST",
      "NULLS_FIRST",
      "NULLS_FIRST",
    ]);
  });
});

describe("CREATE TABLE", () => {
  it("maps declared types by name", () => {
    expect(
      columns(
        "CREATE TABLE t (a varchar(255), b BIGINT, c TINYINT, d DOUBLE, e, f ANY)",
      ).map(([name, type]) => [name, type]),
    ).toEqual([
      ["a", "TEXT"],
      ["b", "BIGINT"],
      ["c", "INTEGER"],
      ["d", "REAL"],
      ["e", "ANY"],
      ["f", "ANY"],
    ]);
  });

  it("refuses an unknown type", () => {
    expect(() => bind("CREATE TABLE t (a WIDGET)")).toThrow(
      new SemanticError("unknown type WIDGET"),
    );
  });

  it("stores constant defaults, signs included", () => {
    expect(
      columns(
        "CREATE TABLE t (a INTEGER DEFAULT -1, b TEXT DEFAULT 'x', c REAL DEFAULT -2.5, d INTEGER DEFAULT NULL, e INTEGER)",
      ).map((column) => column[2]),
    ).toEqual([-1, "x", -2.5, null, null]);
  });

  it("refuses a DEFAULT that is not constant, instead of storing NULL", () => {
    for (const value of ["a", "$1", "(SELECT 1)", "(a + 1)"]) {
      expect(() =>
        bind(`CREATE TABLE t (a INTEGER, b INTEGER DEFAULT ${value})`),
      ).toThrow(SemanticError);
    }
  });

  it("does not evaluate a constant DEFAULT expression yet", () => {
    expect(() => bind("CREATE TABLE t (a INTEGER DEFAULT (1 + 2))")).toThrow(
      NotSupportedError,
    );
    expect(() =>
      bind("ALTER TABLE users ADD COLUMN x TEXT DEFAULT ('a' || 'b')"),
    ).toThrow(NotSupportedError);
  });
});
