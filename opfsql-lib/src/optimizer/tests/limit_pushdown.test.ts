import { beforeEach, describe, expect, it } from "vitest";
import type {
  LogicalLimit,
  LogicalOperator,
  LogicalOrderBy,
} from "../../binder/types.js";
import { LogicalOperatorType } from "../../binder/types.js";
import { pushdownLimit } from "../index.js";
import { createTestContext, findAllNodes, findNode } from "./test_helpers.js";

let bind: (sql: string) => LogicalOperator;

beforeEach(() => {
  const ctx = createTestContext();
  bind = ctx.bind;
});

describe("LimitPushdown", () => {
  it("pushes small LIMIT below projection", () => {
    const plan = bind("SELECT name FROM users LIMIT 10");
    const optimized = pushdownLimit(plan);

    const limits = findAllNodes(optimized, LogicalOperatorType.LOGICAL_LIMIT);
    expect(limits.length).toBeGreaterThanOrEqual(1);
  });

  it("does not push large LIMIT", () => {
    const plan = bind("SELECT name FROM users LIMIT 10000");
    const optimized = pushdownLimit(plan);

    const limit = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_LIMIT,
    ) as LogicalLimit;
    expect(limit).not.toBeNull();
    expect(limit.limitVal).toBe(10000);
  });

  it("annotates ORDER BY with topN when LIMIT is above", () => {
    const plan = bind("SELECT * FROM users ORDER BY age LIMIT 10");
    const optimized = pushdownLimit(plan);

    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    expect(orderBy.topN).toBe(10);
  });

  it("annotates ORDER BY with topN = limit + offset", () => {
    const plan = bind("SELECT * FROM users ORDER BY age LIMIT 5 OFFSET 3");
    const optimized = pushdownLimit(plan);

    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    expect(orderBy.topN).toBe(8);
  });

  it("annotates ORDER BY through PROJECTION", () => {
    const plan = bind("SELECT name FROM users ORDER BY age LIMIT 10");
    const optimized = pushdownLimit(plan);

    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    expect(orderBy.topN).toBe(10);
  });

  it("does not annotate ORDER BY when LIMIT is large", () => {
    const plan = bind("SELECT * FROM users ORDER BY age LIMIT 10000");
    const optimized = pushdownLimit(plan);

    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    expect(orderBy.topN).toBeUndefined();
  });

  it("handles LIMIT 0 edge case", () => {
    const plan = bind("SELECT name FROM users LIMIT 0");
    const optimized = pushdownLimit(plan);

    const limit = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_LIMIT,
    ) as LogicalLimit;
    expect(limit).not.toBeNull();
    expect(limit.limitVal).toBe(0);
  });

  it("handles large OFFSET with small LIMIT", () => {
    const plan = bind("SELECT * FROM users ORDER BY age LIMIT 5 OFFSET 100");
    const optimized = pushdownLimit(plan);

    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    expect(orderBy.topN).toBe(105);
  });

  it("does not push LIMIT below aggregate", () => {
    const plan = bind(
      "SELECT age, COUNT(*) FROM users GROUP BY age LIMIT 5",
    );
    const optimized = pushdownLimit(plan);

    const agg = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
    );
    expect(agg).not.toBeNull();
    const limit = findNode(optimized, LogicalOperatorType.LOGICAL_LIMIT);
    expect(limit).not.toBeNull();
  });
});
