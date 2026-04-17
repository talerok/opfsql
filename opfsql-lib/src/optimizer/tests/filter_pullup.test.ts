import { beforeEach, describe, expect, it } from "vitest";
import type {
  LogicalComparisonJoin,
  LogicalFilter,
  LogicalOperator,
} from "../../binder/types.js";
import { LogicalOperatorType } from "../../binder/types.js";
import { pullupFilters } from "../index.js";
import { createTestContext, findNode } from "./test_helpers.js";

let bind: (sql: string) => LogicalOperator;

beforeEach(() => {
  const ctx = createTestContext();
  bind = ctx.bind;
});

describe("FilterPullup", () => {
  it("pulls INNER JOIN conditions up into filter", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 18",
    );
    const optimized = pullupFilters(plan);

    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    const cross = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_CROSS_PRODUCT,
    );
    expect(cross).not.toBeNull();
  });

  it("preserves INNER JOIN when no WHERE clause exists", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const optimized = pullupFilters(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();
    expect(join.joinType).toBe("INNER");
  });

  it("pulls conditions from all INNER JOINs in a multi-join chain with WHERE", () => {
    const plan = bind(
      `SELECT * FROM users u
       JOIN orders o ON u.id = o.user_id
       JOIN products p ON p.id = o.user_id
       WHERE u.age > 18`,
    );
    const optimized = pullupFilters(plan);

    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    expect(filter.expressions.length).toBeGreaterThanOrEqual(1);
  });

  it("does not pull LEFT JOIN conditions", () => {
    const plan = bind(
      "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
    );
    const optimized = pullupFilters(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();
    expect(join.joinType).toBe("LEFT");
  });
});
