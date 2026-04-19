import { beforeEach, describe, expect, it } from "vitest";
import type {
  BoundConstantExpression,
  LogicalAggregate,
  LogicalOperator,
  LogicalOrderBy,
} from "../../binder/types.js";
import { LogicalOperatorType } from "../../binder/types.js";
import { Catalog } from "../../store/catalog.js";
import { optimize } from "../index.js";
import { createTestContext, findNode, getGet, getScanHint } from "./test_helpers.js";

let catalog: Catalog;
let bind: (sql: string) => LogicalOperator;

beforeEach(() => {
  const ctx = createTestContext();
  catalog = ctx.catalog;
  bind = ctx.bind;
});

describe("IndexSelection", () => {
  it("annotates LogicalGet with indexHint for equality filter", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE age = 30");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    const hint = getScanHint(get);
    expect(hint.indexDef.name).toBe("idx_age");
    expect(hint.predicates).toHaveLength(1);
    expect(hint.predicates[0].comparisonType).toBe("EQUAL");
    expect(
      (hint.predicates[0].value as BoundConstantExpression).value,
    ).toBe(30);
  });

  it("annotates LogicalGet with indexHint for range filter", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE age > 18");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    const hint = getScanHint(get);
    expect(hint.predicates[0].comparisonType).toBe("GREATER");
  });

  it("does NOT set indexHint when no index matches", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE name = 'Alice'");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeUndefined();
  });

  it("does NOT set indexHint when there are no filters", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeUndefined();
  });

  it("prefers unique index over non-unique", () => {
    catalog.addIndex({
      name: "idx_name",
      tableName: "users",
      expressions: [{ type: 'column', name: 'name', returnType: 'TEXT' }],
      unique: false,
    });
    catalog.addIndex({
      name: "idx_name_uniq",
      tableName: "users",
      expressions: [{ type: 'column', name: 'name', returnType: 'TEXT' }],
      unique: true,
    });
    const plan = bind("SELECT * FROM users WHERE name = 'Alice'");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    const hint = getScanHint(get);
    expect(hint.indexDef.name).toBe("idx_name_uniq");
  });

  it("handles composite index with equality prefix", () => {
    catalog.addIndex({
      name: "idx_comp",
      tableName: "orders",
      expressions: [{ type: 'column', name: 'user_id', returnType: 'INTEGER' }, { type: 'column', name: 'status', returnType: 'TEXT' }],
      unique: false,
    });
    const plan = bind(
      "SELECT * FROM orders WHERE user_id = 1 AND status = 'shipped'",
    );
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    const hint = getScanHint(get);
    expect(hint.predicates).toHaveLength(2);
    expect(hint.residualFilters).toHaveLength(0);
  });

  it("sets residual filters for non-covered predicates", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE age = 30 AND name = 'Alice'");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    const hint = getScanHint(get);
    expect(hint.predicates).toHaveLength(1);
    expect(hint.residualFilters).toHaveLength(1);
  });

  it("uses composite index prefix for partial match", () => {
    catalog.addIndex({
      name: "idx_comp",
      tableName: "orders",
      expressions: [{ type: 'column', name: 'user_id', returnType: 'INTEGER' }, { type: 'column', name: 'status', returnType: 'TEXT' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM orders WHERE user_id = 1");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    const hint = getScanHint(get);
    expect(hint.predicates).toHaveLength(1);
    expect(hint.predicates[0].comparisonType).toBe("EQUAL");
  });

  it("does not use composite index when first column has no filter", () => {
    catalog.addIndex({
      name: "idx_comp",
      tableName: "orders",
      expressions: [{ type: 'column', name: 'user_id', returnType: 'INTEGER' }, { type: 'column', name: 'status', returnType: 'TEXT' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM orders WHERE status = 'shipped'");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeUndefined();
  });

  it("handles range filter on single-column index", () => {
    catalog.addIndex({
      name: "idx_amount",
      tableName: "orders",
      expressions: [{ type: 'column', name: 'amount', returnType: 'REAL' }],
      unique: false,
    });
    const plan = bind(
      "SELECT * FROM orders WHERE amount > 50 AND amount < 200",
    );
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    const hint = getScanHint(get);
    expect(hint.predicates.length).toBeGreaterThanOrEqual(1);
  });

  it("chooses index with more covered predicates", () => {
    catalog.addIndex({
      name: "idx_age_only",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    catalog.addIndex({
      name: "idx_age_active",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }, { type: 'column', name: 'active', returnType: 'BOOLEAN' }],
      unique: false,
    });
    const plan = bind(
      "SELECT * FROM users WHERE age = 30 AND active = true",
    );
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    const hint = getScanHint(get);
    expect(hint.indexDef.name).toBe("idx_age_active");
    expect(hint.predicates).toHaveLength(2);
    expect(hint.residualFilters).toHaveLength(0);
  });

  it("matches JSON path index for equality filter", () => {
    catalog.addIndex({
      name: "idx_data_name",
      tableName: "docs",
      expressions: [{
        type: 'json_access',
        column: 'data',
        path: [{ type: 'field', name: 'name' }],
        returnType: 'JSON',
      }],
      unique: false,
    });
    const plan = bind("SELECT * FROM docs WHERE data.name = 'Alice'");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    const hint = getScanHint(get);
    expect(hint.indexDef.name).toBe("idx_data_name");
    expect(hint.predicates).toHaveLength(1);
    expect(hint.predicates[0].comparisonType).toBe("EQUAL");
  });

  it("matches JSON path index for range filter", () => {
    catalog.addIndex({
      name: "idx_data_age",
      tableName: "docs",
      expressions: [{
        type: 'json_access',
        column: 'data',
        path: [{ type: 'field', name: 'age' }],
        returnType: 'JSON',
      }],
      unique: false,
    });
    const plan = bind("SELECT * FROM docs WHERE data.age > 20");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    const hint = getScanHint(get);
    expect(hint.predicates[0].comparisonType).toBe("GREATER");
  });

  it("does NOT match JSON path index when path differs", () => {
    catalog.addIndex({
      name: "idx_data_name",
      tableName: "docs",
      expressions: [{
        type: 'json_access',
        column: 'data',
        path: [{ type: 'field', name: 'name' }],
        returnType: 'JSON',
      }],
      unique: false,
    });
    const plan = bind("SELECT * FROM docs WHERE data.age = 30");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeUndefined();
  });
});

// ===========================================================================
// ORDER BY via Index
// ===========================================================================

describe("ORDER BY via Index", () => {
  it("eliminates Sort when ORDER BY matches index", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users ORDER BY age ASC");
    const optimized = optimize(plan, catalog);
    // Sort node should be removed
    expect(findNode(optimized, LogicalOperatorType.LOGICAL_ORDER_BY)).toBeNull();
    // LogicalGet should have scan hint with empty predicates (order scan)
    const get = getGet(optimized);
    const hint = getScanHint(get);
    expect(hint.indexDef.name).toBe("idx_age");
    expect(hint.predicates).toHaveLength(0);
  });

  it("does NOT eliminate Sort for DESC order", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users ORDER BY age DESC");
    const optimized = optimize(plan, catalog);
    expect(findNode(optimized, LogicalOperatorType.LOGICAL_ORDER_BY)).not.toBeNull();
  });

  it("eliminates Sort for composite index prefix", () => {
    catalog.addIndex({
      name: "idx_comp",
      tableName: "orders",
      expressions: [
        { type: 'column', name: 'user_id', returnType: 'INTEGER' },
        { type: 'column', name: 'status', returnType: 'TEXT' },
      ],
      unique: false,
    });
    const plan = bind("SELECT * FROM orders ORDER BY user_id ASC, status ASC");
    const optimized = optimize(plan, catalog);
    expect(findNode(optimized, LogicalOperatorType.LOGICAL_ORDER_BY)).toBeNull();
    const hint = getScanHint(getGet(optimized));
    expect(hint.indexDef.name).toBe("idx_comp");
  });

  it("does NOT optimize ORDER BY on non-prefix column", () => {
    catalog.addIndex({
      name: "idx_comp",
      tableName: "orders",
      expressions: [
        { type: 'column', name: 'user_id', returnType: 'INTEGER' },
        { type: 'column', name: 'status', returnType: 'TEXT' },
      ],
      unique: false,
    });
    const plan = bind("SELECT * FROM orders ORDER BY status ASC");
    const optimized = optimize(plan, catalog);
    expect(findNode(optimized, LogicalOperatorType.LOGICAL_ORDER_BY)).not.toBeNull();
  });

  it("eliminates Sort when filter and ORDER BY use the same index", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE age > 5 ORDER BY age ASC");
    const optimized = optimize(plan, catalog);
    expect(findNode(optimized, LogicalOperatorType.LOGICAL_ORDER_BY)).toBeNull();
    const hint = getScanHint(getGet(optimized));
    expect(hint.indexDef.name).toBe("idx_age");
    // Should still have the filter predicate
    expect(hint.predicates.length).toBeGreaterThan(0);
  });

  it("works with LIMIT (Sort eliminated, Limit remains)", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users ORDER BY age ASC LIMIT 10");
    const optimized = optimize(plan, catalog);
    expect(findNode(optimized, LogicalOperatorType.LOGICAL_ORDER_BY)).toBeNull();
    expect(findNode(optimized, LogicalOperatorType.LOGICAL_LIMIT)).not.toBeNull();
  });
});

// ===========================================================================
// MIN/MAX via Index
// ===========================================================================

describe("MIN/MAX via Index", () => {
  it("annotates MIN with minMaxHint", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT MIN(age) FROM users");
    const optimized = optimize(plan, catalog);
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(agg).not.toBeNull();
    expect(agg.minMaxHint).toBeDefined();
    expect(agg.minMaxHint!.functionName).toBe("MIN");
    expect(agg.minMaxHint!.indexDef.name).toBe("idx_age");
  });

  it("annotates MAX with minMaxHint", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT MAX(age) FROM users");
    const optimized = optimize(plan, catalog);
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(agg.minMaxHint).toBeDefined();
    expect(agg.minMaxHint!.functionName).toBe("MAX");
  });

  it("does NOT optimize MIN/MAX with GROUP BY", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT name, MIN(age) FROM users GROUP BY name");
    const optimized = optimize(plan, catalog);
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(agg.minMaxHint).toBeUndefined();
  });

  it("does NOT optimize when there are two aggregates", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT MIN(age), MAX(age) FROM users");
    const optimized = optimize(plan, catalog);
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(agg.minMaxHint).toBeUndefined();
  });

  it("does NOT optimize MIN/MAX with filters", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT MIN(age) FROM users WHERE age > 5");
    const optimized = optimize(plan, catalog);
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(agg.minMaxHint).toBeUndefined();
  });

  it("does NOT optimize when no index on aggregated column", () => {
    catalog.addIndex({
      name: "idx_name",
      tableName: "users",
      expressions: [{ type: 'column', name: 'name', returnType: 'TEXT' }],
      unique: false,
    });
    const plan = bind("SELECT MIN(age) FROM users");
    const optimized = optimize(plan, catalog);
    const agg = findNode(optimized, LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY) as LogicalAggregate;
    expect(agg.minMaxHint).toBeUndefined();
  });
});

// ===========================================================================
// OR / Index Union
// ===========================================================================

describe("OR / Index Union", () => {
  it("uses IndexUnion for OR with indexes on both branches", () => {
    catalog.addIndex({
      name: "idx_name",
      tableName: "users",
      expressions: [{ type: 'column', name: 'name', returnType: 'TEXT' }],
      unique: false,
    });
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE name = 'Alice' OR age = 30");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.kind).toBe("union");
    if (get.indexHint!.kind === "union") {
      expect(get.indexHint.branches).toHaveLength(2);
    }
  });

  it("uses IndexUnion for 3-branch OR", () => {
    catalog.addIndex({
      name: "idx_name",
      tableName: "users",
      expressions: [{ type: 'column', name: 'name', returnType: 'TEXT' }],
      unique: false,
    });
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    catalog.addIndex({
      name: "idx_active",
      tableName: "users",
      expressions: [{ type: 'column', name: 'active', returnType: 'BOOLEAN' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE name = 'Alice' OR age = 30 OR active = true");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint?.kind).toBe("union");
    if (get.indexHint?.kind === "union") {
      expect(get.indexHint.branches).toHaveLength(3);
    }
  });

  it("uses same index for OR on same column", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE age = 10 OR age = 20");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint?.kind).toBe("union");
    if (get.indexHint?.kind === "union") {
      expect(get.indexHint.branches).toHaveLength(2);
      expect(get.indexHint.branches[0].indexDef.name).toBe("idx_age");
      expect(get.indexHint.branches[1].indexDef.name).toBe("idx_age");
    }
  });

  it("falls back to full scan when one branch is not indexable", () => {
    catalog.addIndex({
      name: "idx_name",
      tableName: "users",
      expressions: [{ type: 'column', name: 'name', returnType: 'TEXT' }],
      unique: false,
    });
    // age has no index, so the OR cannot be fully covered
    const plan = bind("SELECT * FROM users WHERE name = 'Alice' OR age = 30");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    // Should not have an index union hint
    expect(get.indexHint?.kind).not.toBe("union");
  });

  it("removes Filter node when OR is the only filter expression", () => {
    catalog.addIndex({
      name: "idx_name",
      tableName: "users",
      expressions: [{ type: 'column', name: 'name', returnType: 'TEXT' }],
      unique: false,
    });
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE name = 'Alice' OR age = 30");
    const optimized = optimize(plan, catalog);
    // Filter node should be removed since the OR was the only expression
    expect(findNode(optimized, LogicalOperatorType.LOGICAL_FILTER)).toBeNull();
  });
});
