import { beforeEach, describe, expect, it } from "vitest";
import type {
  LogicalComparisonJoin,
  LogicalOperator,
} from "../../binder/types.js";
import { LogicalOperatorType } from "../../binder/types.js";
import { optimizeBuildProbeSide } from "../index.js";
import { createTestContext, findNode, getAllGets, getGet } from "./test_helpers.js";

let bind: (sql: string) => LogicalOperator;

beforeEach(() => {
  const ctx = createTestContext();
  bind = ctx.bind;
});

describe("BuildProbeSideOptimizer", () => {
  it("swaps sides when left is smaller (INNER JOIN)", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const gets = getAllGets(plan);
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 10;
      if (get.tableName === "orders") get.estimatedCardinality = 10000;
    }

    const optimized = optimizeBuildProbeSide(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();

    const rightGet = getGet(join.children[1]);
    expect(rightGet.tableName).toBe("users");
  });

  it("does not swap LEFT JOIN", () => {
    const plan = bind(
      "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
    );
    const gets = getAllGets(plan);
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 10;
      if (get.tableName === "orders") get.estimatedCardinality = 10000;
    }

    const optimized = optimizeBuildProbeSide(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    const leftGet = getGet(join.children[0]);
    expect(leftGet.tableName).toBe("users");
  });

  it("does not swap when left is already larger (build=right is smaller)", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const gets = getAllGets(plan);
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 10000;
      if (get.tableName === "orders") get.estimatedCardinality = 10;
    }

    const optimized = optimizeBuildProbeSide(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    const rightGet = getGet(join.children[1]);
    expect(rightGet.tableName).toBe("orders");
  });

  it("handles equal cardinality without swapping", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const gets = getAllGets(plan);
    for (const get of gets) {
      get.estimatedCardinality = 100;
    }

    const optimized = optimizeBuildProbeSide(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();
    expect(getAllGets(optimized)).toHaveLength(2);
  });
});
