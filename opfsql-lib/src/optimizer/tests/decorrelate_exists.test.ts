import { beforeEach, describe, expect, it } from "vitest";
import type {
  LogicalComparisonJoin,
  LogicalFilter,
  LogicalOperator,
} from "../../binder/types.js";
import {
  BoundExpressionClass,
  LogicalOperatorType,
} from "../../binder/types.js";
import { Catalog } from "../../store/catalog.js";
import {
  decorrelateExists,
  optimize,
  pushdownFilters,
  removeUnusedColumns,
} from "../index.js";
import { createTestContext, findNode } from "./test_helpers.js";

let catalog: Catalog;
let bind: (sql: string) => LogicalOperator;

beforeEach(() => {
  const ctx = createTestContext();
  catalog = ctx.catalog;
  bind = ctx.bind;
});

describe("decorrelateExists", () => {
  it("transforms EXISTS into SEMI join", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100)",
    );
    const optimized = decorrelateExists(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("SEMI");
    expect(join.conditions).toHaveLength(1);
    expect(join.conditions[0].comparisonType).toBe("EQUAL");

    const filter = findNode(optimized, LogicalOperatorType.LOGICAL_FILTER);
    if (filter) {
      expect(filter.expressions[0].expressionClass).not.toBe(
        BoundExpressionClass.BOUND_SUBQUERY,
      );
    }
  });

  it("transforms NOT EXISTS into ANTI join", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    const optimized = decorrelateExists(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("ANTI");
  });

  it("preserves non-EXISTS conditions alongside EXISTS", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE u.age > 18 AND EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    const optimized = decorrelateExists(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("SEMI");

    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).toBeTruthy();
    expect(filter.expressions[0].expressionClass).toBe(
      BoundExpressionClass.BOUND_COMPARISON,
    );
  });

  it("SEMI join output has only outer columns", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    const optimized = decorrelateExists(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();

    const outerTypes = join.children[0].types;
    expect(join.types).toEqual(outerTypes);

    const outerBindings = join.children[0].columnBindings;
    expect(join.columnBindings).toEqual(outerBindings);
  });

  it("does not decorrelate uncorrelated EXISTS", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.amount > 100)",
    );
    const optimized = decorrelateExists(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    expect(join).toBeNull();
  });

  it("full optimize pipeline produces correct SEMI join", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100)",
    );
    const optimized = optimize(plan, catalog);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("SEMI");
  });

  it("handles EXISTS with multiple correlated conditions", () => {
    const plan = bind(
      `SELECT u.name FROM users u WHERE EXISTS (
        SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = u.name
      )`,
    );
    const optimized = decorrelateExists(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("SEMI");
    expect(join.conditions).toHaveLength(2);
  });

  it("ANTI join output has only outer columns", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    const optimized = decorrelateExists(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("ANTI");

    const outerTypes = join.children[0].types;
    expect(join.types).toEqual(outerTypes);
  });
});

describe("Recursive CTE optimization", () => {
  it("removeUnusedColumns preserves all columns in recursive CTE anchor and recursive children", () => {
    const plan = bind(
      "WITH RECURSIVE cnt(n, label) AS (SELECT 1, 'a' UNION ALL SELECT n + 1, 'a' FROM cnt WHERE n < 3) SELECT n, label FROM cnt",
    );
    const optimized = removeUnusedColumns(plan);

    const recCTE = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_RECURSIVE_CTE,
    );
    expect(recCTE).toBeTruthy();

    const anchorBindings = recCTE!.children[0].columnBindings;
    expect(anchorBindings.length).toBe(2);

    const recBindings = recCTE!.children[1]!.columnBindings;
    expect(recBindings.length).toBeGreaterThanOrEqual(2);
  });

  it("filter pushdown optimizes inside recursive CTE children independently", () => {
    const plan = bind(
      "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT n FROM r WHERE n > 5",
    );
    const optimized = pushdownFilters(plan);

    const recCTE = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_RECURSIVE_CTE,
    );
    expect(recCTE).toBeTruthy();
  });

  it("full optimize pipeline preserves multi-column recursive CTE", () => {
    const plan = bind(
      "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 20) SELECT a, b FROM fib",
    );
    const optimized = optimize(plan, catalog);

    const recCTE = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_RECURSIVE_CTE,
    );
    expect(recCTE).toBeTruthy();

    const anchorBindings = recCTE!.children[0].columnBindings;
    expect(anchorBindings.length).toBe(2);
  });
});
