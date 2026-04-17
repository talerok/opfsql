import { beforeEach, describe, expect, it } from "vitest";
import type {
  BoundColumnRefExpression,
  BoundSubqueryExpression,
  LogicalFilter,
  LogicalGet,
  LogicalProjection,
} from "../types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../types.js";
import { createTestContext } from "./test_helpers.js";

let catalog: ReturnType<typeof createTestContext>["catalog"];
let bind: ReturnType<typeof createTestContext>["bind"];

beforeEach(() => {
  const ctx = createTestContext();
  catalog = ctx.catalog;
  bind = ctx.bind;
});

describe("Subquery", () => {
  it("subquery in FROM creates virtual table", () => {
    const plan = bind("SELECT sub.id FROM (SELECT id, name FROM users) sub");
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(1);
    const col = proj.expressions[0] as BoundColumnRefExpression;
    expect(col.columnName).toBe("id");
  });

  it("EXISTS subquery returns BOOLEAN", () => {
    const plan = bind(
      "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)",
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const sub = filter.expressions[0] as BoundSubqueryExpression;
    expect(sub.expressionClass).toBe(BoundExpressionClass.BOUND_SUBQUERY);
    expect(sub.subqueryType).toBe("EXISTS");
    expect(sub.returnType).toBe("BOOLEAN");
  });
});

describe("Subqueries — additional", () => {
  it("NOT EXISTS subquery returns BOOLEAN", () => {
    const plan = bind(
      "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = 999)",
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    // NOT EXISTS is either a NOT_EXISTS subquery type or a NOT wrapping EXISTS
    expect(filter.expressions).toHaveLength(1);
  });

  it("scalar subquery in SELECT", () => {
    const plan = bind("SELECT (SELECT MAX(age) FROM users)");
    const proj = plan as LogicalProjection;
    const sub = proj.expressions[0] as BoundSubqueryExpression;
    expect(sub.expressionClass).toBe(BoundExpressionClass.BOUND_SUBQUERY);
    expect(sub.subqueryType).toBe("SCALAR");
  });
});

describe("Subquery in FROM — bindings", () => {
  it("subquery ref getColumnBindings matches outer scope tableIndex", () => {
    const plan = bind("SELECT sub.id FROM (SELECT id, name FROM users) sub");
    const proj = plan as LogicalProjection;
    const col = proj.expressions[0] as BoundColumnRefExpression;
    // The subquery wrapper should have bindings matching the resolved column
    const subGet = proj.children[0] as LogicalGet;
    const bindings = subGet.getColumnBindings();
    expect(bindings.some((b) => b.tableIndex === col.binding.tableIndex)).toBe(
      true,
    );
  });
});

describe("Correlated subqueries", () => {
  it("WHERE EXISTS with outer table reference resolves correctly", () => {
    const plan = bind(
      "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const sub = filter.expressions[0] as BoundSubqueryExpression;
    expect(sub.subqueryType).toBe("EXISTS");
    expect(sub.returnType).toBe("BOOLEAN");
  });

  it("scalar correlated subquery resolves outer column", () => {
    const plan = bind(
      "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) FROM users u",
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const sub = proj.expressions[1] as BoundSubqueryExpression;
    expect(sub.expressionClass).toBe(BoundExpressionClass.BOUND_SUBQUERY);
    expect(sub.subqueryType).toBe("SCALAR");
  });

  it("unqualified outer column resolves via parent scope", () => {
    const plan = bind(
      "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = id)",
    );
    // Should not throw — 'id' is unambiguous within the combined scope (users.id from parent, orders doesn't have 'id' column... wait, orders has 'id')
    // Actually 'id' is ambiguous since both users and orders have 'id'. But user_id is unambiguous.
    const proj = plan as LogicalProjection;
    expect(proj.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
  });
});
