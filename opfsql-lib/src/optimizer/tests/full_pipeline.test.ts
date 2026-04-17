import { beforeEach, describe, expect, it } from "vitest";
import type {
  BoundConstantExpression,
  LogicalComparisonJoin,
  LogicalFilter,
  LogicalOperator,
  LogicalOrderBy,
} from "../../binder/types.js";
import {
  BoundExpressionClass,
  LogicalOperatorType,
} from "../../binder/types.js";
import { optimize } from "../index.js";
import {
  createTestContext,
  findNode,
  getAllGets,
  getGet,
} from "./test_helpers.js";

let bind: (sql: string) => LogicalOperator;

beforeEach(() => {
  const ctx = createTestContext();
  bind = ctx.bind;
});

describe("optimize (full pipeline)", () => {
  it("optimizes simple select with filter", () => {
    const plan = bind("SELECT name FROM users WHERE age > 18");
    const optimized = optimize(plan);
    const proj = findNode(optimized, LogicalOperatorType.LOGICAL_PROJECTION);
    expect(proj).not.toBeNull();
    const get = getGet(optimized);
    expect(get.tableName).toBe("users");
  });

  it("optimizes join with filter pushdown", () => {
    const plan = bind(
      "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 18",
    );
    const optimized = optimize(plan);
    const usersGet = getAllGets(optimized).find((g) => g.tableName === "users");
    expect(usersGet).toBeDefined();
    expect(usersGet!.tableFilters.length).toBeGreaterThan(0);
  });

  it("optimizes cross product to join", () => {
    const plan = bind(
      "SELECT * FROM users CROSS JOIN orders WHERE users.id = orders.user_id",
    );
    const optimized = optimize(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    expect(join).not.toBeNull();
  });

  it("handles constant folding end-to-end", () => {
    const plan = bind("SELECT * FROM users WHERE 1 + 1 = 2");
    const optimized = optimize(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter | null;
    if (filter) {
      const expr = filter.expressions[0];
      if (expr.expressionClass === BoundExpressionClass.BOUND_CONSTANT) {
        expect((expr as BoundConstantExpression).value).toBe(true);
      }
    }
    const get = getGet(optimized);
    expect(get.tableName).toBe("users");
  });

  it("optimizes query with LIMIT", () => {
    const plan = bind("SELECT name FROM users LIMIT 5");
    const optimized = optimize(plan);
    const limit = findNode(optimized, LogicalOperatorType.LOGICAL_LIMIT);
    expect(limit).not.toBeNull();
  });

  it("optimizes query with GROUP BY", () => {
    const plan = bind(
      "SELECT age, COUNT(*) FROM users WHERE age > 18 GROUP BY age",
    );
    const optimized = optimize(plan);
    const agg = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
    );
    expect(agg).not.toBeNull();
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
  });

  it("preserves DDL statements", () => {
    const plan = bind("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    const optimized = optimize(plan);
    expect(optimized.type).toBe(LogicalOperatorType.LOGICAL_CREATE_TABLE);
  });

  it("preserves DML insert", () => {
    const plan = bind(
      "INSERT INTO users (id, name, age) VALUES (1, 'John', 30)",
    );
    const optimized = optimize(plan);
    expect(optimized.type).toBe(LogicalOperatorType.LOGICAL_INSERT);
  });

  it("optimizes subquery in WHERE", () => {
    const plan = bind(
      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
    );
    const optimized = optimize(plan);
    const get = getAllGets(optimized);
    expect(get.length).toBeGreaterThanOrEqual(1);
  });

  it("CTE + JOIN produces COMPARISON_JOIN not CROSS_PRODUCT", () => {
    const plan = bind(
      `WITH rev AS (SELECT user_id, SUM(amount) AS total_amount FROM orders GROUP BY user_id)
       SELECT u.name, r.total_amount
       FROM rev r INNER JOIN users u ON r.user_id = u.id
       ORDER BY r.total_amount DESC LIMIT 10`,
    );
    const optimized = optimize(plan);
    const cross = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_CROSS_PRODUCT,
    );
    expect(cross).toBeNull();
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    expect(join).not.toBeNull();
  });

  it("optimizes ORDER BY with LIMIT (topN annotation)", () => {
    const plan = bind(
      "SELECT name FROM users ORDER BY age DESC LIMIT 3",
    );
    const optimized = optimize(plan);
    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    expect(orderBy.topN).toBe(3);
  });

  it("optimizes DISTINCT query", () => {
    const plan = bind("SELECT DISTINCT name FROM users");
    const optimized = optimize(plan);
    const distinct = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_DISTINCT,
    );
    expect(distinct).not.toBeNull();
  });

  it("optimizes UPDATE statement", () => {
    const plan = bind("UPDATE users SET age = 30 WHERE id = 1");
    const optimized = optimize(plan);
    expect(optimized.type).toBe(LogicalOperatorType.LOGICAL_UPDATE);
  });

  it("optimizes DELETE statement", () => {
    const plan = bind("DELETE FROM users WHERE age < 18");
    const optimized = optimize(plan);
    expect(optimized.type).toBe(LogicalOperatorType.LOGICAL_DELETE);
  });
});
