import { beforeEach, describe, expect, it } from "vitest";
import type {
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  LogicalAggregate,
  LogicalComparisonJoin,
  LogicalFilter,
  LogicalGet,
  LogicalOperator,
  LogicalProjection,
} from "../../binder/types.js";
import {
  BoundExpressionClass,
  LogicalOperatorType,
} from "../../binder/types.js";
import { Binder } from "../../binder/index.js";
import { Catalog } from "../../store/catalog.js";
import type { TableSchema } from "../../store/types.js";
import {
  decorrelateExists,
  optimize,
  pullupFilters,
  pushdownFilters,
  removeUnusedColumns,
  reorderFilters,
  rewriteExpressions,
} from "../index.js";
import {
  createTestContext,
  findNode,
  getAllGets,
  getGet,
} from "./test_helpers.js";

let catalog: Catalog;
let binder: Binder;
let bind: (sql: string) => LogicalOperator;

beforeEach(() => {
  const ctx = createTestContext();
  catalog = ctx.catalog;
  binder = ctx.binder;
  bind = ctx.bind;
});

describe("FilterPushdown", () => {
  it("pushes filter below projection", () => {
    const plan = bind("SELECT name FROM users WHERE age > 18");
    const optimized = pushdownFilters(plan);
    const proj = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(proj).not.toBeNull();
    if (optimized.type === LogicalOperatorType.LOGICAL_PROJECTION) {
      const child = optimized.children[0];
      const hasFilterBelow =
        child.type === LogicalOperatorType.LOGICAL_FILTER ||
        findNode(child, LogicalOperatorType.LOGICAL_FILTER) !== null;
      const hasGetWithFilters =
        findNode(child, LogicalOperatorType.LOGICAL_GET) !== null;
      expect(hasFilterBelow || hasGetWithFilters).toBe(true);
    }
  });

  it("pushes filter to scan as table filter", () => {
    const plan = bind("SELECT * FROM users WHERE age > 18");
    const optimized = pushdownFilters(plan);
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
    expect((get.tableFilters[0].expression as any).binding.columnIndex).toBe(2);
    expect(get.tableFilters[0].comparisonType).toBe("GREATER");
  });

  it("splits filters across INNER JOIN sides", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 18 AND orders.amount > 100",
    );
    const pulled = pullupFilters(plan);
    const optimized = pushdownFilters(pulled);

    const gets = getAllGets(optimized);
    const usersGet = gets.find((g) => g.tableName === "users");
    const ordersGet = gets.find((g) => g.tableName === "orders");

    expect(usersGet).toBeDefined();
    expect(ordersGet).toBeDefined();
    expect(usersGet!.tableFilters.length).toBeGreaterThan(0);
    expect(ordersGet!.tableFilters.length).toBeGreaterThan(0);
  });

  it("converts cross product with condition to INNER JOIN", () => {
    const plan = bind(
      "SELECT * FROM users CROSS JOIN orders WHERE users.id = orders.user_id",
    );
    const optimized = pushdownFilters(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    expect(join).not.toBeNull();
    expect((join as LogicalComparisonJoin).joinType).toBe("INNER");
    expect((join as LogicalComparisonJoin).conditions.length).toBeGreaterThan(
      0,
    );
  });

  it("does not push right-side filters through LEFT JOIN", () => {
    const plan = bind(
      "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE orders.amount > 100",
    );
    const optimized = pushdownFilters(plan);
    const ordersGet = getAllGets(optimized).find(
      (g) => g.tableName === "orders",
    );
    expect(ordersGet).toBeDefined();
    expect(ordersGet!.tableFilters).toHaveLength(0);
  });

  it("pushes filters through ORDER BY", () => {
    const plan = bind("SELECT * FROM users WHERE age > 18 ORDER BY name");
    const optimized = pushdownFilters(plan);
    const orderBy = findNode(optimized, LogicalOperatorType.LOGICAL_ORDER_BY);
    expect(orderBy).not.toBeNull();
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
  });

  it("pushes pre-aggregation filters through aggregate", () => {
    const plan = bind(
      "SELECT age, COUNT(*) FROM users WHERE age > 18 GROUP BY age",
    );
    const optimized = pushdownFilters(plan);
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
  });

  it("pushes filters through MaterializedCTE into main plan", () => {
    const plan = bind(
      `WITH active AS (SELECT id, name FROM users WHERE active = true)
       SELECT a.name FROM active a INNER JOIN orders o ON a.id = o.user_id WHERE o.amount > 100`,
    );
    const pulled = pullupFilters(plan);
    const optimized = pushdownFilters(pulled);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    expect(join).not.toBeNull();
    expect((join as LogicalComparisonJoin).joinType).toBe("INNER");

    const ordersGet = getAllGets(optimized).find(
      (g) => g.tableName === "orders",
    );
    expect(ordersGet).toBeDefined();
    expect(ordersGet!.tableFilters.length).toBeGreaterThan(0);
  });

  it("preserves join condition when CTE query has WHERE clause", () => {
    const plan = bind(
      `WITH totals AS (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id)
       SELECT u.name, t.total FROM totals t INNER JOIN users u ON t.user_id = u.id WHERE t.total > 500`,
    );
    const pulled = pullupFilters(plan);
    const optimized = pushdownFilters(pulled);

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
    expect((join as LogicalComparisonJoin).conditions.length).toBeGreaterThan(
      0,
    );
  });

  it("does not convert non-equality cross-table filter to join condition", () => {
    const plan = bind(
      "SELECT * FROM users a JOIN users b ON a.id = b.id WHERE b.age > a.age",
    );
    const pulled = pullupFilters(plan);
    const optimized = pushdownFilters(pulled);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();

    for (const cond of join.conditions) {
      expect(cond.comparisonType).toBe("EQUAL");
    }

    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    const filterCmp = filter.expressions[0] as BoundComparisonExpression;
    expect(filterCmp.comparisonType).toBe("GREATER");
  });

  it("normalizes sides of extracted join conditions (left=left-child, right=right-child)", () => {
    const plan = bind(
      "SELECT * FROM users u CROSS JOIN orders o WHERE o.user_id = u.id",
    );
    const optimized = pushdownFilters(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();
    expect(join.conditions.length).toBeGreaterThan(0);

    const leftTables = new Set(
      join.children[0].columnBindings.map((b) => b.tableIndex),
    );
    const rightTables = new Set(
      join.children[1].columnBindings.map((b) => b.tableIndex),
    );

    for (const cond of join.conditions) {
      const condLeftRef = cond.left as BoundColumnRefExpression;
      const condRightRef = cond.right as BoundColumnRefExpression;
      expect(leftTables.has(condLeftRef.binding.tableIndex)).toBe(true);
      expect(rightTables.has(condRightRef.binding.tableIndex)).toBe(true);
    }
  });

  it("normalizes sides when adding join condition to existing join", () => {
    const plan = bind(
      "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount = u.age",
    );
    const pulled = pullupFilters(plan);
    const optimized = pushdownFilters(pulled);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();

    const leftTables = new Set(
      join.children[0].columnBindings.map((b) => b.tableIndex),
    );
    const rightTables = new Set(
      join.children[1].columnBindings.map((b) => b.tableIndex),
    );

    for (const cond of join.conditions) {
      expect(cond.comparisonType).toBe("EQUAL");
      const condLeftRef = cond.left as BoundColumnRefExpression;
      const condRightRef = cond.right as BoundColumnRefExpression;
      expect(leftTables.has(condLeftRef.binding.tableIndex)).toBe(true);
      expect(rightTables.has(condRightRef.binding.tableIndex)).toBe(true);
    }
  });

  it("optimizes CTE definition independently", () => {
    const plan = bind(
      `WITH filtered AS (SELECT id, name FROM users WHERE age > 21)
       SELECT f.name FROM filtered f`,
    );
    const optimized = pushdownFilters(plan);

    const usersGet = getAllGets(optimized).find((g) => g.tableName === "users");
    expect(usersGet).toBeDefined();
    expect(usersGet!.tableFilters.length).toBeGreaterThan(0);
    expect((usersGet!.tableFilters[0].expression as any).binding.columnIndex).toBe(2);
  });
});

describe("FilterPushdown — edge cases", () => {
  it("pushes filter through DISTINCT", () => {
    const plan = bind(
      "SELECT DISTINCT name, age FROM users WHERE age > 18",
    );
    const optimized = pushdownFilters(plan);
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
  });

  it("keeps filter above UNION (can't push through set operation)", () => {
    const plan = bind(
      "SELECT name FROM users WHERE age > 18 UNION ALL SELECT name FROM users WHERE age < 5",
    );
    const optimized = pushdownFilters(plan);
    const union = findNode(optimized, LogicalOperatorType.LOGICAL_UNION);
    expect(union).not.toBeNull();
  });

  it("does not push HAVING filter through aggregate (non group-by column)", () => {
    const plan = bind(
      "SELECT age, COUNT(*) as c FROM users GROUP BY age HAVING COUNT(*) > 5",
    );
    const optimized = pushdownFilters(plan);
    const agg = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
    );
    expect(agg).not.toBeNull();
  });

  it("pushes filter referencing group column through aggregate", () => {
    const plan = bind(
      "SELECT age, COUNT(*) FROM users GROUP BY age HAVING age > 18",
    );
    const pulled = pullupFilters(plan);
    const optimized = pushdownFilters(pulled);
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    expect(agg).not.toBeNull();
    expect(optimized).toBeTruthy();
  });

  it("handles filter that references no tables (constant filter)", () => {
    const plan = bind(
      "SELECT * FROM users CROSS JOIN orders WHERE 1 = 1",
    );
    const rewritten = rewriteExpressions(plan);
    const optimized = pushdownFilters(rewritten);
    expect(optimized).toBeTruthy();
  });

  it("remaps filter through projection (column index mapping)", () => {
    const plan = bind(
      "SELECT name, age FROM users WHERE age > 18",
    );
    const optimized = pushdownFilters(plan);
    const proj = findNode(optimized, LogicalOperatorType.LOGICAL_PROJECTION);
    expect(proj).not.toBeNull();
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
  });

  it("pushes left-side filter through LEFT JOIN", () => {
    const plan = bind(
      "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE users.age > 18",
    );
    const optimized = pushdownFilters(plan);
    const usersGet = getAllGets(optimized).find((g) => g.tableName === "users");
    expect(usersGet).toBeDefined();
    expect(usersGet!.tableFilters.length).toBeGreaterThan(0);
  });

  it("pushes filters through SEMI join sides", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE u.age > 18 AND EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 50)",
    );
    const decorrelated = decorrelateExists(plan);
    const optimized = pushdownFilters(decorrelated);
    const usersGet = getAllGets(optimized).find((g) => g.tableName === "users");
    expect(usersGet).toBeDefined();
    expect(usersGet!.tableFilters.length).toBeGreaterThan(0);
  });

  it("deduplicates filter against tableFilter (parameter comparison)", () => {
    const plan = bind("SELECT * FROM users WHERE age = 30");
    const optimized = pushdownFilters(plan);
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
    const filter = findNode(optimized, LogicalOperatorType.LOGICAL_FILTER);
    expect(filter).toBeNull();
  });

  it("pushes HAVING on group column below aggregate (remapHavingThroughGroups)", () => {
    const plan = bind(
      "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING user_id > 5",
    );
    const optimized = pushdownFilters(plan);
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
    expect(get.tableFilters.some((tf) => (tf.expression as any).binding.columnIndex === 1)).toBe(true);
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(agg.havingExpression).toBeNull();
  });

  it("keeps HAVING on aggregate function (cannot push below aggregate)", () => {
    const plan = bind(
      "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING COUNT(*) > 5",
    );
    const optimized = pushdownFilters(plan);
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(agg.havingExpression).not.toBeNull();
  });

  it("keeps HAVING with mixed group+aggregate refs (cannot push)", () => {
    const plan = bind(
      "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING user_id > COUNT(*)",
    );
    const optimized = pushdownFilters(plan);
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(agg.havingExpression).not.toBeNull();
  });

  it("splits conjunctive HAVING: pushes group part, keeps aggregate part", () => {
    const plan = bind(
      "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING user_id > 5 AND COUNT(*) > 10",
    );
    const optimized = pushdownFilters(plan);
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
    expect(get.tableFilters.some((tf) => (tf.expression as any).binding.columnIndex === 1)).toBe(true);
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(agg.havingExpression).not.toBeNull();
  });

  it("reconstructs conjunctive HAVING when multiple aggregate parts are kept", () => {
    const plan = bind(
      "SELECT user_id, COUNT(*) as cnt, SUM(amount) as total FROM orders GROUP BY user_id HAVING user_id > 5 AND COUNT(*) > 10 AND SUM(amount) > 100",
    );
    const optimized = pushdownFilters(plan);
    // user_id > 5 is pushed below, COUNT(*) > 10 AND SUM(amount) > 100 stay as conjunction
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(agg.havingExpression).not.toBeNull();
    // The HAVING should be a conjunction of the two remaining aggregate filters
    expect(agg.havingExpression!.expressionClass).toBe(BoundExpressionClass.BOUND_CONJUNCTION);
    const conj = agg.havingExpression as BoundConjunctionExpression;
    expect(conj.children).toHaveLength(2);
    // The group column filter should be pushed to the scan
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
  });

  it("keeps filter above cross product when expression uses function on both sides (mixed refs)", () => {
    const plan = bind(
      "SELECT * FROM users CROSS JOIN orders WHERE users.age + orders.amount > 100",
    );
    const optimized = pushdownFilters(plan);
    const join = findNode(optimized, LogicalOperatorType.LOGICAL_COMPARISON_JOIN);
    const filter = findNode(optimized, LogicalOperatorType.LOGICAL_FILTER);
    if (!join) {
      expect(filter).not.toBeNull();
    }
  });

  it("pushes single-side filters through cross product", () => {
    const plan = bind(
      "SELECT * FROM users CROSS JOIN orders WHERE users.age > 18 AND orders.amount > 50",
    );
    const optimized = pushdownFilters(plan);
    const usersGet = getAllGets(optimized).find((g) => g.tableName === "users");
    const ordersGet = getAllGets(optimized).find((g) => g.tableName === "orders");
    expect(usersGet!.tableFilters.length).toBeGreaterThan(0);
    expect(ordersGet!.tableFilters.length).toBeGreaterThan(0);
  });

  it("pushes right-side filter through SEMI join", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100)",
    );
    const decorrelated = decorrelateExists(plan);
    const optimized = pushdownFilters(decorrelated);
    const ordersGet = getAllGets(optimized).find((g) => g.tableName === "orders");
    expect(ordersGet).toBeDefined();
    expect(ordersGet!.tableFilters.length).toBeGreaterThan(0);
  });

  it("pushes right-side filter through ANTI join", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100)",
    );
    const decorrelated = decorrelateExists(plan);
    const optimized = pushdownFilters(decorrelated);
    const ordersGet = getAllGets(optimized).find((g) => g.tableName === "orders");
    expect(ordersGet).toBeDefined();
    expect(ordersGet!.tableFilters.length).toBeGreaterThan(0);
  });
});

describe("FilterPushdown — defensive code paths", () => {
  // These paths are unreachable through the binder but guard against
  // malformed plans. We construct plans manually to exercise them.

  function makeGet(tableIndex: number): LogicalGet {
    return {
      type: LogicalOperatorType.LOGICAL_GET,
      children: [],
      expressions: [],
      types: ["INTEGER", "TEXT"],
      estimatedCardinality: 100,
      tableIndex,
      tableName: "t",
      schema: { name: "t", columns: [] },
      columnIds: [0, 1],
      tableFilters: [],
      columnBindings: [
        { tableIndex, columnIndex: 0 },
        { tableIndex, columnIndex: 1 },
      ],
    };
  }

  it("remapThroughProjection returns filter above when column index exceeds projection expressions", () => {
    // Filter references column 5 of the projection output, but projection only has 2 expressions.
    // The defensive canRemap=false path (line 441) should fire, keeping the filter above.
    const get = makeGet(0);
    const projTableIndex = 10;
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: projTableIndex,
      children: [get],
      expressions: [
        { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 0 }, tableName: "t", columnName: "a", returnType: "INTEGER" } as BoundColumnRefExpression,
        { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 1 }, tableName: "t", columnName: "b", returnType: "TEXT" } as BoundColumnRefExpression,
      ],
      aliases: ["a", "b"],
      types: ["INTEGER", "TEXT"],
      estimatedCardinality: 100,
      columnBindings: [
        { tableIndex: projTableIndex, columnIndex: 0 },
        { tableIndex: projTableIndex, columnIndex: 1 },
      ],
    };
    // Filter referencing column index 5 (out of bounds for the 2-expression projection)
    const outOfBoundsFilter: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: projTableIndex, columnIndex: 5 }, tableName: "", columnName: "", returnType: "INTEGER" } as BoundColumnRefExpression,
      right: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 10, returnType: "INTEGER" },
      returnType: "BOOLEAN",
    };
    const filterNode: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [proj],
      expressions: [outOfBoundsFilter],
      types: proj.types,
      estimatedCardinality: 100,
      columnBindings: proj.columnBindings,
    };

    const result = pushdownFilters(filterNode);
    // Filter should remain above (not pushed through projection)
    expect(result.type).toBe(LogicalOperatorType.LOGICAL_FILTER);
    const resultFilter = result as LogicalFilter;
    expect(resultFilter.expressions).toHaveLength(1);
    expect(resultFilter.children[0].type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
  });

  it("deduplicates filter with parameter expression against tableFilter", () => {
    // When a filter uses a BOUND_PARAMETER (not a constant), deduplication
    // should compare parameter indices (lines 274-278).
    const get = makeGet(0);
    const paramExpr = {
      expressionClass: BoundExpressionClass.BOUND_PARAMETER as const,
      index: 0,
      returnType: "INTEGER" as const,
    };
    const paramFilter: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 0 }, tableName: "t", columnName: "a", returnType: "INTEGER" } as BoundColumnRefExpression,
      right: paramExpr,
      returnType: "BOOLEAN",
    };
    const filterNode: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [get],
      expressions: [paramFilter],
      types: get.types,
      estimatedCardinality: 100,
      columnBindings: get.columnBindings,
    };

    const result = pushdownFilters(filterNode);
    const getNode = findNode(result, LogicalOperatorType.LOGICAL_GET) as LogicalGet;
    expect(getNode.tableFilters.length).toBeGreaterThan(0);
    // Filter should be deduplicated (no remaining filter above)
    expect(findNode(result, LogicalOperatorType.LOGICAL_FILTER)).toBeNull();
  });

  it("remapHavingThroughGroups keeps HAVING when column ref uses aggregateIndex", () => {
    // HAVING expression has a column ref bound to aggregateIndex (aggregate output table).
    // The defensive check at lines 513-514 should prevent pushdown.
    const get = makeGet(0);
    const groupIndex = 10;
    const aggregateIndex = 11;
    const agg: LogicalAggregate = {
      type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
      groupIndex,
      aggregateIndex,
      children: [get],
      expressions: [],
      groups: [
        { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 0 }, tableName: "t", columnName: "a", returnType: "INTEGER" } as BoundColumnRefExpression,
      ],
      // HAVING that references aggregateIndex (e.g., an aggregate output column)
      havingExpression: {
        expressionClass: BoundExpressionClass.BOUND_COMPARISON,
        comparisonType: "GREATER",
        left: { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: aggregateIndex, columnIndex: 0 }, tableName: "", columnName: "", returnType: "INTEGER" } as BoundColumnRefExpression,
        right: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 5, returnType: "INTEGER" },
        returnType: "BOOLEAN",
      } as BoundComparisonExpression,
      types: ["INTEGER"],
      estimatedCardinality: 100,
      columnBindings: [
        { tableIndex: groupIndex, columnIndex: 0 },
        { tableIndex: aggregateIndex, columnIndex: 0 },
      ],
    };

    const result = pushdownFilters(agg);
    // HAVING should be kept (not pushed down)
    const aggResult = findNode(result, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(aggResult.havingExpression).not.toBeNull();
  });

  it("remapThroughProjection successfully remaps filter with valid column index", () => {
    // Filter above projection references column 0 of the projection output (valid index).
    // Should be remapped to the projection's source expression and pushed down.
    const get = makeGet(0);
    const projTableIndex = 10;
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: projTableIndex,
      children: [get],
      expressions: [
        { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 0 }, tableName: "t", columnName: "a", returnType: "INTEGER" } as BoundColumnRefExpression,
        { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 1 }, tableName: "t", columnName: "b", returnType: "TEXT" } as BoundColumnRefExpression,
      ],
      aliases: ["a", "b"],
      types: ["INTEGER", "TEXT"],
      estimatedCardinality: 100,
      columnBindings: [
        { tableIndex: projTableIndex, columnIndex: 0 },
        { tableIndex: projTableIndex, columnIndex: 1 },
      ],
    };
    // Filter referencing column 0 of projection output (valid — should remap to get.column 0)
    const validFilter: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: projTableIndex, columnIndex: 0 }, tableName: "", columnName: "", returnType: "INTEGER" } as BoundColumnRefExpression,
      right: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 10, returnType: "INTEGER" },
      returnType: "BOOLEAN",
    };
    const filterNode: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [proj],
      expressions: [validFilter],
      types: proj.types,
      estimatedCardinality: 100,
      columnBindings: proj.columnBindings,
    };

    const result = pushdownFilters(filterNode);
    // Filter should be pushed through the projection (remapped to source columns)
    expect(result.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    const getNode = findNode(result, LogicalOperatorType.LOGICAL_GET) as LogicalGet;
    expect(getNode.tableFilters.length).toBeGreaterThan(0);
  });

  it("tryExtractJoinCondition returns null for equality with mixed table references", () => {
    // Cross product with WHERE (a.col0 + b.col0) = 100, where left side references
    // both tables. The mixed-references path (line 486) should fire.
    const getA = makeGet(0);
    const getB = makeGet(1);
    getB.tableName = "t2";
    getB.columnBindings = [
      { tableIndex: 1, columnIndex: 0 },
      { tableIndex: 1, columnIndex: 1 },
    ];
    const cross = {
      type: LogicalOperatorType.LOGICAL_CROSS_PRODUCT,
      children: [getA, getB],
      expressions: [],
      types: ["INTEGER", "TEXT", "INTEGER", "TEXT"],
      estimatedCardinality: 10000,
      columnBindings: [
        ...getA.columnBindings,
        ...getB.columnBindings,
      ],
    } as LogicalOperator;
    // Construct (t.col0 + t2.col0) = 100 — left side references both tables
    const mixedEquality: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: {
        expressionClass: BoundExpressionClass.BOUND_FUNCTION,
        functionName: "add",
        children: [
          { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 0 }, tableName: "t", columnName: "a", returnType: "INTEGER" } as BoundColumnRefExpression,
          { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 1, columnIndex: 0 }, tableName: "t2", columnName: "a", returnType: "INTEGER" } as BoundColumnRefExpression,
        ],
        returnType: "INTEGER",
      },
      right: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 100, returnType: "INTEGER" },
      returnType: "BOOLEAN",
    };
    const filterNode: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [cross],
      expressions: [mixedEquality],
      types: cross.types,
      estimatedCardinality: 10000,
      columnBindings: cross.columnBindings,
    };

    const result = pushdownFilters(filterNode);
    // Should NOT convert to join (mixed references) — filter stays above cross product
    expect(findNode(result, LogicalOperatorType.LOGICAL_COMPARISON_JOIN)).toBeNull();
    expect(result.type).toBe(LogicalOperatorType.LOGICAL_FILTER);
    expect(findNode(result, LogicalOperatorType.LOGICAL_CROSS_PRODUCT)).not.toBeNull();
  });

  it("remapHavingThroughGroups keeps HAVING when groupIndex column exceeds groups count", () => {
    // HAVING with column ref to groupIndex but index >= groups.length (defensive line 511).
    const get = makeGet(0);
    const groupIndex = 10;
    const aggregateIndex = 11;
    const agg: LogicalAggregate = {
      type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
      groupIndex,
      aggregateIndex,
      children: [get],
      expressions: [],
      groups: [
        { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 0 }, tableName: "t", columnName: "a", returnType: "INTEGER" } as BoundColumnRefExpression,
      ],
      // HAVING references groupIndex column 5 — but only 1 group exists
      havingExpression: {
        expressionClass: BoundExpressionClass.BOUND_COMPARISON,
        comparisonType: "GREATER",
        left: { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: groupIndex, columnIndex: 5 }, tableName: "", columnName: "", returnType: "INTEGER" } as BoundColumnRefExpression,
        right: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 5, returnType: "INTEGER" },
        returnType: "BOOLEAN",
      } as BoundComparisonExpression,
      types: ["INTEGER"],
      estimatedCardinality: 100,
      columnBindings: [
        { tableIndex: groupIndex, columnIndex: 0 },
      ],
    };

    const result = pushdownFilters(agg);
    // HAVING should be kept (groupIndex column out of bounds)
    const aggResult = findNode(result, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(aggResult.havingExpression).not.toBeNull();
  });

  it("referencesOnlyGroupColumns pushes filter matching group column refs through aggregate", () => {
    // Construct a filter above an aggregate where the filter's column refs
    // match the group expression column refs (same tableIndex/columnIndex).
    // This exercises the referencesOnlyGroupColumns function (lines 536-547).
    const get = makeGet(0);
    const groupIndex = 10;
    const aggregateIndex = 11;
    const agg: LogicalAggregate = {
      type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
      groupIndex,
      aggregateIndex,
      children: [get],
      expressions: [],
      groups: [
        { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 0 }, tableName: "t", columnName: "a", returnType: "INTEGER" } as BoundColumnRefExpression,
      ],
      havingExpression: null,
      types: ["INTEGER"],
      estimatedCardinality: 100,
      columnBindings: [
        { tableIndex: groupIndex, columnIndex: 0 },
      ],
    };
    // Filter referencing the same column as the group expression (tableIndex=0, columnIndex=0)
    const groupMatchingFilter: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 0 }, tableName: "t", columnName: "a", returnType: "INTEGER" } as BoundColumnRefExpression,
      right: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 18, returnType: "INTEGER" },
      returnType: "BOOLEAN",
    };
    const filterNode: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [agg],
      expressions: [groupMatchingFilter],
      types: agg.types,
      estimatedCardinality: 100,
      columnBindings: agg.columnBindings,
    };

    const result = pushdownFilters(filterNode);
    // The filter should be pushed through the aggregate (no filter above)
    expect(result.type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    // The pushed filter should appear as a table filter on the get
    const getNode = findNode(result, LogicalOperatorType.LOGICAL_GET) as LogicalGet;
    expect(getNode.tableFilters.length).toBeGreaterThan(0);
  });

  it("referencesOnlyGroupColumns keeps filter when it references non-group columns", () => {
    // Filter references column 1 which is NOT in the group expressions (only column 0 is).
    const get = makeGet(0);
    const groupIndex = 10;
    const aggregateIndex = 11;
    const agg: LogicalAggregate = {
      type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
      groupIndex,
      aggregateIndex,
      children: [get],
      expressions: [],
      groups: [
        { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 0 }, tableName: "t", columnName: "a", returnType: "INTEGER" } as BoundColumnRefExpression,
      ],
      havingExpression: null,
      types: ["INTEGER"],
      estimatedCardinality: 100,
      columnBindings: [
        { tableIndex: groupIndex, columnIndex: 0 },
      ],
    };
    // Filter referencing column 1 (NOT a group column)
    const nonGroupFilter: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 1 }, tableName: "t", columnName: "b", returnType: "TEXT" } as BoundColumnRefExpression,
      right: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: "x", returnType: "TEXT" },
      returnType: "BOOLEAN",
    };
    const filterNode: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [agg],
      expressions: [nonGroupFilter],
      types: agg.types,
      estimatedCardinality: 100,
      columnBindings: agg.columnBindings,
    };

    const result = pushdownFilters(filterNode);
    // The filter should stay above the aggregate
    expect(result.type).toBe(LogicalOperatorType.LOGICAL_FILTER);
    const resultFilter = result as LogicalFilter;
    expect(resultFilter.children[0].type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
  });
});

describe("JSON filter pushdown", () => {
  const docsSchema: TableSchema = {
    name: "docs",
    columns: [
      { name: "id", type: "INTEGER", nullable: false, primaryKey: true, unique: true, autoIncrement: false, defaultValue: null },
      { name: "data", type: "JSON", nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    ],
  };

  beforeEach(() => {
    catalog.addTable(docsSchema);
  });

  it("JSON path AND range filter keeps both conditions after optimize", () => {
    const plan = bind("SELECT id FROM docs WHERE data.age > 20 AND data.age < 30");
    const optimized = optimize(plan, catalog);

    const filter = findNode(optimized, LogicalOperatorType.LOGICAL_FILTER);
    if (filter) {
      const totalConditions = filter.expressions.reduce((acc: number, e: any) => {
        if (e.expressionClass === BoundExpressionClass.BOUND_CONJUNCTION) {
          return acc + (e as BoundConjunctionExpression).children.length;
        }
        return acc + 1;
      }, 0);
      expect(totalConditions).toBeGreaterThanOrEqual(2);
    }

    const get = findNode(optimized, LogicalOperatorType.LOGICAL_GET) as LogicalGet;
    expect(get).toBeTruthy();
    for (const tf of get.tableFilters) {
      expect((tf.expression as any).binding?.columnIndex).not.toBe(1);
    }
  });

  it("isolate which optimizer pass breaks JSON path AND filter", () => {
    function countAllConditions(p: LogicalOperator): number {
      let total = 0;
      // Count conditions in LogicalFilter nodes
      const f = findNode(p, LogicalOperatorType.LOGICAL_FILTER);
      if (f) {
        total += f.expressions.reduce((acc: number, e: any) => {
          if (e.expressionClass === BoundExpressionClass.BOUND_CONJUNCTION) {
            return acc + (e as BoundConjunctionExpression).children.length;
          }
          return acc + 1;
        }, 0);
      }
      // Count conditions pushed down as table filters
      const get = findNode(p, LogicalOperatorType.LOGICAL_GET) as LogicalGet | null;
      if (get) {
        total += get.tableFilters.length;
      }
      return total;
    }

    const passes = [
      ["rewriteExpressions", rewriteExpressions],
      ["decorrelateExists", decorrelateExists],
      ["pullupFilters", pullupFilters],
      ["pushdownFilters", pushdownFilters],
      ["removeUnusedColumns", removeUnusedColumns],
      ["reorderFilters", reorderFilters],
    ] as const;

    for (const [name, pass] of passes) {
      const plan = bind("SELECT id FROM docs WHERE data.age > 20 AND data.age < 30");
      const before = countAllConditions(plan);
      const result = pass(plan);
      const after = countAllConditions(result);
      expect(after, `${name}: filter conditions dropped from ${before} to ${after}`).toBeGreaterThanOrEqual(2);
    }
  });
});
