import { beforeEach, describe, expect, it } from "vitest";
import type {
  BoundColumnRefExpression,
  BoundSubqueryExpression,
  LogicalFilter,
  LogicalInsert,
  LogicalLimit,
  LogicalMaterializedCTE,
  LogicalOrderBy,
  LogicalProjection,
  LogicalUnion,
} from "../types.js";
import { LogicalOperatorType } from "../types.js";
import { createTestContext } from "./test_helpers.js";

let catalog: ReturnType<typeof createTestContext>["catalog"];
let bind: ReturnType<typeof createTestContext>["bind"];

beforeEach(() => {
  const ctx = createTestContext();
  catalog = ctx.catalog;
  bind = ctx.bind;
});

describe("UNION", () => {
  it("UNION ALL produces LogicalUnion with all = true", () => {
    const plan = bind(
      "SELECT id, name FROM users UNION ALL SELECT id, status FROM orders",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_UNION);
    const union = plan as LogicalUnion;
    expect(union.all).toBe(true);
    expect(union.children).toHaveLength(2);
  });

  it("UNION (without ALL) produces LogicalUnion with all = false", () => {
    const plan = bind(
      "SELECT id, name FROM users UNION SELECT id, status FROM orders",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_UNION);
    const union = plan as LogicalUnion;
    expect(union.all).toBe(false);
  });
});

describe("UNION — additional", () => {
  it("UNION with ORDER BY and LIMIT", () => {
    const plan = bind(
      "SELECT id, name FROM users UNION ALL SELECT id, status FROM orders ORDER BY id LIMIT 5",
    );
    // Top should be LIMIT
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_LIMIT);
    const limit = plan as LogicalLimit;
    expect(limit.limitVal).toBe(5);
    // Next should be ORDER BY
    expect(limit.children[0].type).toBe(LogicalOperatorType.LOGICAL_ORDER_BY);
    // Then UNION
    const orderBy = limit.children[0] as LogicalOrderBy;
    expect(orderBy.children[0].type).toBe(LogicalOperatorType.LOGICAL_UNION);
  });
});

describe("UNION inside subqueries", () => {
  it("FROM subquery containing UNION ALL binds correctly", () => {
    const plan = bind(
      "SELECT sub.id, sub.name FROM (SELECT id, name FROM users UNION ALL SELECT id, status FROM orders) sub",
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    expect(col0.columnName).toBe("id");
  });

  it("CTE body containing UNION ALL binds correctly", () => {
    const plan = bind(
      "WITH combined AS (SELECT id, name FROM users UNION ALL SELECT id, status FROM orders) SELECT * FROM combined",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const cte = plan as LogicalMaterializedCTE;
    expect(cte.cteName).toBe("combined");
    const mainProj = cte.children[1] as LogicalProjection;
    expect(mainProj.expressions).toHaveLength(2);
  });

  it("EXISTS subquery containing UNION ALL binds correctly", () => {
    const plan = bind(
      "SELECT * FROM users WHERE EXISTS (SELECT id FROM users UNION ALL SELECT id FROM orders)",
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const sub = filter.expressions[0] as BoundSubqueryExpression;
    expect(sub.subqueryType).toBe("EXISTS");
    expect(sub.returnType).toBe("BOOLEAN");
  });

  it("IN subquery containing UNION ALL binds correctly", () => {
    const plan = bind(
      "SELECT * FROM users WHERE id IN (SELECT id FROM users UNION ALL SELECT id FROM orders)",
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    expect(filter.expressions).toHaveLength(1);
  });

  it("INSERT INTO ... SELECT ... UNION ALL binds correctly", () => {
    const plan = bind(
      "INSERT INTO orders (id, user_id, amount, status) SELECT id, id, age, name FROM users UNION ALL SELECT id, user_id, amount, status FROM orders",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_INSERT);
    const ins = plan as LogicalInsert;
    expect(ins.tableName).toBe("orders");
    expect(ins.children).toHaveLength(1);
  });

  it("scalar subquery containing UNION ALL binds correctly", () => {
    const plan = bind(
      "SELECT (SELECT MAX(id) FROM users UNION ALL SELECT MAX(id) FROM orders) FROM users",
    );
    const proj = plan as LogicalProjection;
    const sub = proj.expressions[0] as BoundSubqueryExpression;
    expect(sub.subqueryType).toBe("SCALAR");
  });
});
