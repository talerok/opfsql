import { beforeEach, describe, expect, it } from "vitest";
import type {
  BoundComparisonExpression,
  BoundConstantExpression,
  LogicalFilter,
  LogicalOperator,
} from "../../binder/types.js";
import {
  BoundExpressionClass,
  LogicalOperatorType,
} from "../../binder/types.js";
import { rewriteExpressions } from "../index.js";
import { createTestContext, findNode } from "./test_helpers.js";

let bind: (sql: string) => LogicalOperator;

beforeEach(() => {
  const ctx = createTestContext();
  bind = ctx.bind;
});

describe("ExpressionRewriter", () => {
  describe("constant folding", () => {
    it("folds 1 + 1 = 2 to true", () => {
      const plan = bind("SELECT * FROM users WHERE 1 + 1 = 2");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        true,
      );
    });

    it("folds arithmetic: 2 * 3 + 1 in comparison", () => {
      const plan = bind("SELECT * FROM users WHERE age > 2 * 3 + 1");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.right.expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((cmp.right as BoundConstantExpression).value).toBe(7);
    });

    it("folds string equality", () => {
      const plan = bind("SELECT * FROM users WHERE 'abc' = 'abc'");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        true,
      );
    });
  });

  describe("comparison simplification", () => {
    it("simplifies NULL = x to NULL", () => {
      const plan = bind("SELECT * FROM users WHERE NULL = age");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        null,
      );
    });
  });

  describe("conjunction simplification", () => {
    it("simplifies x AND true to x", () => {
      const plan = bind("SELECT * FROM users WHERE age > 18 AND true");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const expr = filter.expressions[0];
      expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_COMPARISON);
      expect((expr as BoundComparisonExpression).comparisonType).toBe(
        "GREATER",
      );
    });

    it("simplifies x AND false to false", () => {
      const plan = bind("SELECT * FROM users WHERE age > 18 AND false");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        false,
      );
    });

    it("simplifies x OR true to true", () => {
      const plan = bind("SELECT * FROM users WHERE age > 18 OR true");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        true,
      );
    });

    it("simplifies x OR false to x", () => {
      const plan = bind("SELECT * FROM users WHERE age > 18 OR false");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const expr = filter.expressions[0];
      expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_COMPARISON);
    });
  });

  describe("arithmetic simplification", () => {
    it("simplifies x + 0 to x", () => {
      const plan = bind("SELECT * FROM users WHERE age + 0 > 18");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
    });

    it("simplifies x * 1 to x", () => {
      const plan = bind("SELECT * FROM users WHERE age * 1 > 18");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
    });

    it("simplifies x * 0 to CASE WHEN x IS NOT NULL THEN 0 ELSE NULL (NULL-safe)", () => {
      const plan = bind("SELECT * FROM users WHERE age * 0 = 0");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.left.expressionClass).toBe(BoundExpressionClass.BOUND_CASE);
    });

    it("folds constant * 0 to 0 directly", () => {
      const plan = bind("SELECT * FROM users WHERE 5 * 0 = 0");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        true,
      );
    });
  });

  describe("move constants", () => {
    it("normalizes constant to right side: 5 < age → age > 5", () => {
      const plan = bind("SELECT * FROM users WHERE 5 < age");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.comparisonType).toBe("GREATER");
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
      expect(cmp.right.expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
    });

    it("moves arithmetic constant: age + 3 < 10 → age < 7", () => {
      const plan = bind("SELECT * FROM users WHERE age + 3 < 10");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
      expect(cmp.right.expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((cmp.right as BoundConstantExpression).value).toBe(7);
    });

    it("moves subtract constant: age - 3 < 10 → age < 13", () => {
      const plan = bind("SELECT * FROM users WHERE age - 3 < 10");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
      expect(cmp.right.expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((cmp.right as BoundConstantExpression).value).toBe(13);
    });
  });

  describe("additional simplifications", () => {
    it("folds false OR x to x", () => {
      const plan = bind("SELECT * FROM users WHERE false OR age > 18");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const expr = filter.expressions[0];
      expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_COMPARISON);
    });

    it("folds nested constant arithmetic: (2 + 3) * 4 = 20 → true", () => {
      const plan = bind("SELECT * FROM users WHERE (2 + 3) * 4 = 20");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        true,
      );
    });

    it("simplifies x / 1 to x", () => {
      const plan = bind("SELECT * FROM users WHERE age / 1 > 18");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
    });
  });
});
