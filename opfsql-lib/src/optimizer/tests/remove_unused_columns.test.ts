import { beforeEach, describe, expect, it } from "vitest";
import type {
  LogicalOperator,
  LogicalProjection,
} from "../../binder/types.js";
import { LogicalOperatorType } from "../../binder/types.js";
import { pushdownFilters, removeUnusedColumns } from "../index.js";
import { createTestContext, findNode, getAllGets, getGet } from "./test_helpers.js";

let bind: (sql: string) => LogicalOperator;

beforeEach(() => {
  const ctx = createTestContext();
  bind = ctx.bind;
});

describe("RemoveUnusedColumns", () => {
  it("prunes unused columns from scan", () => {
    const plan = bind("SELECT name FROM users");
    const optimized = removeUnusedColumns(plan);
    const get = getGet(optimized);
    expect(get.columnIds.length).toBeLessThan(4);
  });

  it("keeps columns referenced by WHERE", () => {
    const plan = bind("SELECT name FROM users WHERE age > 18");
    const optimized = removeUnusedColumns(plan);
    const get = getGet(optimized);
    expect(get.columnIds).toContain(1); // name
    expect(get.columnIds).toContain(2); // age
  });

  it("keeps columns referenced by ORDER BY", () => {
    const plan = bind("SELECT name, age FROM users ORDER BY age");
    const optimized = removeUnusedColumns(plan);
    const get = getGet(optimized);
    expect(get.columnIds).toContain(1); // name
    expect(get.columnIds).toContain(2); // age
  });

  it("keeps columns referenced by JOIN conditions", () => {
    const plan = bind(
      "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id",
    );
    const optimized = removeUnusedColumns(plan);
    const usersGet = getAllGets(optimized).find(
      (g) => g.tableName === "users",
    )!;
    expect(usersGet.columnIds).toContain(0); // id (used in join)
    expect(usersGet.columnIds).toContain(1); // name (used in select)
  });

  it("keeps at least one column even if none are used", () => {
    const plan = bind("SELECT 1 FROM users");
    const optimized = removeUnusedColumns(plan);
    const get = getGet(optimized);
    expect(get.columnIds.length).toBeGreaterThanOrEqual(1);
  });

  it("keeps columns referenced by tableFilters after pushdown", () => {
    const plan = bind("SELECT name FROM users WHERE age > 18");
    const pushed = pushdownFilters(plan);
    const optimized = removeUnusedColumns(pushed);
    const get = getGet(optimized);

    expect(get.tableFilters.length).toBeGreaterThan(0);
    expect(get.columnIds).toContain(2); // age
    expect(get.columnIds).toContain(1); // name
  });

  it("preserves aliases through column pruning", () => {
    const plan = bind("SELECT name AS username, age AS user_age FROM users");
    const proj = findNode(
      plan,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(proj.aliases).toEqual(["username", "user_age"]);

    const optimized = removeUnusedColumns(plan);
    const prunedProj = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(prunedProj.aliases).toEqual(["username", "user_age"]);
    expect(prunedProj.expressions).toHaveLength(2);
  });

  it("prunes aliases in parallel with expressions", () => {
    const plan = bind("SELECT name AS username FROM users");
    const optimized = removeUnusedColumns(plan);
    const proj = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(proj.expressions).toHaveLength(1);
    expect(proj.aliases).toHaveLength(1);
    expect(proj.aliases[0]).toBe("username");
  });

  it("preserves null aliases through pruning", () => {
    const plan = bind("SELECT name, age FROM users");
    const optimized = removeUnusedColumns(plan);
    const proj = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(proj.aliases).toEqual([null, null]);
    expect(proj.aliases).toHaveLength(proj.expressions.length);
  });

  it("preserves mixed aliases through pruning", () => {
    const plan = bind("SELECT name AS username, age FROM users");
    const optimized = removeUnusedColumns(plan);
    const proj = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(proj.aliases).toEqual(["username", null]);
  });
});
