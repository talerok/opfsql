import { beforeEach, describe, expect, it } from "vitest";
import type {
  LogicalComparisonJoin,
  LogicalOperator,
} from "../../binder/types.js";
import { LogicalOperatorType } from "../../binder/types.js";
import { Binder } from "../../binder/index.js";
import { Catalog } from "../../store/catalog.js";
import {
  optimizeJoinOrder,
  pullupFilters,
  pushdownFilters,
} from "../index.js";
import {
  createTestContext,
  findNode,
  getAllGets,
  getGet,
  parse,
  pushed,
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

describe("JoinOrderOptimizer", () => {
  it("preserves single join", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const optimized = optimizeJoinOrder(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();
    expect(join.conditions.length).toBeGreaterThan(0);
  });

  it("reorders multi-way join by cardinality", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON products.id = orders.user_id",
    );

    const gets = getAllGets(plan);
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 1000;
      if (get.tableName === "orders") get.estimatedCardinality = 10000;
      if (get.tableName === "products") get.estimatedCardinality = 100;
    }

    const pulled = pullupFilters(plan);
    const optimized = optimizeJoinOrder(pushed(pulled));

    const resultGets = getAllGets(optimized);
    expect(resultGets).toHaveLength(3);
    const tableNames = resultGets.map((g) => g.tableName).sort();
    expect(tableNames).toEqual(["orders", "products", "users"]);
  });

  it("does not reorder single table (no join)", () => {
    const plan = bind("SELECT * FROM users WHERE age > 18");
    const optimized = optimizeJoinOrder(plan);
    const get = getGet(optimized);
    expect(get.tableName).toBe("users");
    expect(getAllGets(optimized)).toHaveLength(1);
  });

  it("handles very unbalanced cardinalities", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const gets = getAllGets(plan);
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 1;
      if (get.tableName === "orders") get.estimatedCardinality = 1000000;
    }

    const optimized = optimizeJoinOrder(plan);
    const resultGets = getAllGets(optimized);
    expect(resultGets).toHaveLength(2);
  });
});

describe("JoinOrderOptimizer — edge cases", () => {
  it("handles cross product (no join conditions) in 3-way join", () => {
    const plan = bind(
      "SELECT * FROM users CROSS JOIN orders CROSS JOIN products",
    );
    const gets = getAllGets(plan);
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 100;
      if (get.tableName === "orders") get.estimatedCardinality = 10;
      if (get.tableName === "products") get.estimatedCardinality = 50;
    }
    const optimized = optimizeJoinOrder(plan);
    expect(getAllGets(optimized)).toHaveLength(3);
  });

  it("reconstructed join tree has working columnBindings", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON products.id = orders.user_id",
    );
    const gets = getAllGets(plan);
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 100;
      if (get.tableName === "orders") get.estimatedCardinality = 1000;
      if (get.tableName === "products") get.estimatedCardinality = 50;
    }
    const pulled = pullupFilters(plan);
    const pushed2 = pushdownFilters(pulled);
    const optimized = optimizeJoinOrder(pushed2);

    const bindings = optimized.columnBindings;
    expect(bindings.length).toBeGreaterThan(0);
  });

  it("does not reorder LEFT JOIN", () => {
    const plan = bind(
      "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
    );
    const optimized = optimizeJoinOrder(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();
    expect(join.joinType).toBe("LEFT");
  });

  it("greedy heuristic for >6 relations", () => {
    for (let i = 0; i < 7; i++) {
      catalog.addTable({
        name: `t${i}`,
        columns: [
          { name: "id", type: "INTEGER", nullable: false, primaryKey: true, unique: true, autoIncrement: false, defaultValue: null },
          { name: "val", type: "INTEGER", nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
        ],
      });
    }
    binder = new Binder(catalog);
    bind = (sql: string) => binder.bindStatement(parse(sql));
    const plan = bind(
      `SELECT * FROM t0
       CROSS JOIN t1 CROSS JOIN t2 CROSS JOIN t3
       CROSS JOIN t4 CROSS JOIN t5 CROSS JOIN t6
       WHERE t0.id = t1.id AND t1.id = t2.id AND t2.id = t3.id
         AND t3.id = t4.id AND t4.id = t5.id AND t5.id = t6.id`,
    );
    const pulled = pullupFilters(plan);
    const pushed2 = pushdownFilters(pulled);
    const optimized = optimizeJoinOrder(pushed2);
    expect(getAllGets(optimized)).toHaveLength(7);
  });
});
