import { beforeEach, describe, expect, it } from "vitest";
import type {
  BoundComparisonExpression,
  BoundConjunctionExpression,
  LogicalFilter,
  LogicalOperator,
} from "../../binder/types.js";
import {
  BoundExpressionClass,
  LogicalOperatorType,
} from "../../binder/types.js";
import { rewriteInClauses } from "../index.js";
import { createTestContext, findNode } from "./test_helpers.js";

let bind: (sql: string) => LogicalOperator;

beforeEach(() => {
  const ctx = createTestContext();
  bind = ctx.bind;
});

describe("InClauseRewriter", () => {
  it("rewrites single-value IN to equality", () => {
    const plan = bind("SELECT * FROM users WHERE id IN (5)");
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    const expr = filter.expressions[0];
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_COMPARISON);
    expect((expr as BoundComparisonExpression).comparisonType).toBe("EQUAL");
  });

  it("rewrites multi-value IN to OR", () => {
    const plan = bind("SELECT * FROM users WHERE id IN (1, 2, 3)");
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    const expr = filter.expressions[0];
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_CONJUNCTION);
    const conj = expr as BoundConjunctionExpression;
    expect(conj.conjunctionType).toBe("OR");
    expect(conj.children).toHaveLength(3);
  });

  it("rewrites NOT IN to AND of NOT_EQUAL", () => {
    const plan = bind("SELECT * FROM users WHERE id NOT IN (1, 2)");
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    const expr = filter.expressions[0];
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_CONJUNCTION);
    const conj = expr as BoundConjunctionExpression;
    expect(conj.conjunctionType).toBe("AND");
    expect(conj.children).toHaveLength(2);
    for (const child of conj.children) {
      expect((child as BoundComparisonExpression).comparisonType).toBe(
        "NOT_EQUAL",
      );
    }
  });

  it("does not expand large IN list (>10 values)", () => {
    const values = Array.from({ length: 11 }, (_, i) => i + 1).join(", ");
    const plan = bind(`SELECT * FROM users WHERE id IN (${values})`);
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    const expr = filter.expressions[0];
    expect(expr.expressionClass).not.toBe(
      BoundExpressionClass.BOUND_CONJUNCTION,
    );
  });

  it("expands IN with exactly 10 values (at threshold)", () => {
    const values = Array.from({ length: 10 }, (_, i) => i + 1).join(", ");
    const plan = bind(`SELECT * FROM users WHERE id IN (${values})`);
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    const expr = filter.expressions[0];
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_CONJUNCTION);
    expect((expr as BoundConjunctionExpression).conjunctionType).toBe("OR");
    expect((expr as BoundConjunctionExpression).children).toHaveLength(10);
  });

  it("rewrites NOT IN single value to NOT_EQUAL", () => {
    const plan = bind("SELECT * FROM users WHERE id NOT IN (5)");
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    const expr = filter.expressions[0];
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_COMPARISON);
    expect((expr as BoundComparisonExpression).comparisonType).toBe(
      "NOT_EQUAL",
    );
  });
});
