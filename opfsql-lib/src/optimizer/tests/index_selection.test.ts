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
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
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
      expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
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
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.indexDef.name).toBe("idx_name_uniq");
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
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates).toHaveLength(2);
    expect(get.indexHint!.residualFilters).toHaveLength(0);
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
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates).toHaveLength(1);
    expect(get.indexHint!.residualFilters).toHaveLength(1);
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
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates).toHaveLength(1);
    expect(get.indexHint!.predicates[0].comparisonType).toBe("EQUAL");
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
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates.length).toBeGreaterThanOrEqual(1);
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
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.indexDef.name).toBe("idx_age_active");
    expect(get.indexHint!.predicates).toHaveLength(2);
    expect(get.indexHint!.residualFilters).toHaveLength(0);
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
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.indexDef.name).toBe("idx_data_name");
    expect(get.indexHint!.predicates).toHaveLength(1);
    expect(get.indexHint!.predicates[0].comparisonType).toBe("EQUAL");
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
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates[0].comparisonType).toBe("GREATER");
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
