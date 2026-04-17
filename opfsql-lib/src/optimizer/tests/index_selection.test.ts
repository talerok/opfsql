import { beforeEach, describe, expect, it } from "vitest";
import type {
  BoundConstantExpression,
  LogicalOperator,
} from "../../binder/types.js";
import { Catalog } from "../../store/catalog.js";
import { optimize } from "../index.js";
import { createTestContext, getGet } from "./test_helpers.js";

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
      columns: ["age"],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE age = 30");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.indexDef.name).toBe("idx_age");
    expect(get.indexHint!.predicates).toHaveLength(1);
    expect(get.indexHint!.predicates[0].comparisonType).toBe("EQUAL");
    expect(
      (get.indexHint!.predicates[0].value as BoundConstantExpression).value,
    ).toBe(30);
  });

  it("annotates LogicalGet with indexHint for range filter", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      columns: ["age"],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE age > 18");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates[0].comparisonType).toBe("GREATER");
  });

  it("does NOT set indexHint when no index matches", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      columns: ["age"],
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
      columns: ["age"],
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
      columns: ["name"],
      unique: false,
    });
    catalog.addIndex({
      name: "idx_name_uniq",
      tableName: "users",
      columns: ["name"],
      unique: true,
    });
    const plan = bind("SELECT * FROM users WHERE name = 'Alice'");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.indexDef.name).toBe("idx_name_uniq");
  });

  it("handles composite index with equality prefix", () => {
    catalog.addIndex({
      name: "idx_comp",
      tableName: "orders",
      columns: ["user_id", "status"],
      unique: false,
    });
    const plan = bind(
      "SELECT * FROM orders WHERE user_id = 1 AND status = 'shipped'",
    );
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates).toHaveLength(2);
    expect(get.indexHint!.residualFilters).toHaveLength(0);
  });

  it("sets residual filters for non-covered predicates", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      columns: ["age"],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE age = 30 AND name = 'Alice'");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates).toHaveLength(1);
    expect(get.indexHint!.residualFilters).toHaveLength(1);
  });

  it("uses composite index prefix for partial match", () => {
    catalog.addIndex({
      name: "idx_comp",
      tableName: "orders",
      columns: ["user_id", "status"],
      unique: false,
    });
    const plan = bind("SELECT * FROM orders WHERE user_id = 1");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates).toHaveLength(1);
    expect(get.indexHint!.predicates[0].comparisonType).toBe("EQUAL");
  });

  it("does not use composite index when first column has no filter", () => {
    catalog.addIndex({
      name: "idx_comp",
      tableName: "orders",
      columns: ["user_id", "status"],
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
      columns: ["amount"],
      unique: false,
    });
    const plan = bind(
      "SELECT * FROM orders WHERE amount > 50 AND amount < 200",
    );
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates.length).toBeGreaterThanOrEqual(1);
  });

  it("chooses index with more covered predicates", () => {
    catalog.addIndex({
      name: "idx_age_only",
      tableName: "users",
      columns: ["age"],
      unique: false,
    });
    catalog.addIndex({
      name: "idx_age_active",
      tableName: "users",
      columns: ["age", "active"],
      unique: false,
    });
    const plan = bind(
      "SELECT * FROM users WHERE age = 30 AND active = true",
    );
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.indexDef.name).toBe("idx_age_active");
    expect(get.indexHint!.predicates).toHaveLength(2);
    expect(get.indexHint!.residualFilters).toHaveLength(0);
  });
});
