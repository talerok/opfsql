import { beforeEach, describe, expect, it } from "vitest";
import { BindError } from "../core/errors.js";
import type {
  BoundColumnRefExpression,
  LogicalComparisonJoin,
  LogicalCrossProduct,
  LogicalGet,
  LogicalProjection,
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

describe("JOIN", () => {
  it("INNER JOIN builds LogicalComparisonJoin with joinType INNER", () => {
    const plan = bind(
      "SELECT u.name FROM users u INNER JOIN orders o ON u.id = o.user_id",
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    expect(join.type).toBe(LogicalOperatorType.LOGICAL_COMPARISON_JOIN);
    expect(join.joinType).toBe("INNER");
  });

  it("LEFT JOIN builds LogicalComparisonJoin with joinType LEFT", () => {
    const plan = bind(
      "SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id",
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    expect(join.type).toBe(LogicalOperatorType.LOGICAL_COMPARISON_JOIN);
    expect(join.joinType).toBe("LEFT");
  });

  it("columns from both tables are accessible after JOIN", () => {
    const plan = bind(
      "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id",
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    expect(col0.tableName).toBe("users");
    expect(col0.columnName).toBe("name");
    const col1 = proj.expressions[1] as BoundColumnRefExpression;
    expect(col1.tableName).toBe("orders");
    expect(col1.columnName).toBe("amount");
  });

  it("JOIN ON condition resolves columns from both tables", () => {
    const plan = bind(
      "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id",
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    expect(join.conditions).toHaveLength(1);
    expect(join.conditions[0].comparisonType).toBe("EQUAL");
    const leftCol = join.conditions[0].left as BoundColumnRefExpression;
    const rightCol = join.conditions[0].right as BoundColumnRefExpression;
    expect(leftCol.tableName).toBe("users");
    expect(rightCol.tableName).toBe("orders");
  });

  it("CROSS JOIN builds LogicalCrossProduct", () => {
    const plan = bind("SELECT * FROM users CROSS JOIN orders");
    const proj = plan as LogicalProjection;
    const cross = proj.children[0] as LogicalCrossProduct;
    expect(cross.type).toBe(LogicalOperatorType.LOGICAL_CROSS_PRODUCT);
  });
});

describe("JOIN — additional", () => {
  it("USING clause resolves columns from left and right tables", () => {
    const plan = bind("SELECT * FROM users JOIN orders USING (id)");
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    expect(join.type).toBe(LogicalOperatorType.LOGICAL_COMPARISON_JOIN);
    expect(join.conditions).toHaveLength(1);
    expect(join.conditions[0].comparisonType).toBe("EQUAL");
    const leftCol = join.conditions[0].left as BoundColumnRefExpression;
    const rightCol = join.conditions[0].right as BoundColumnRefExpression;
    expect(leftCol.tableName).toBe("users");
    expect(rightCol.tableName).toBe("orders");
  });

  it("RIGHT JOIN throws BindError", () => {
    expect(() =>
      bind(
        "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id",
      ),
    ).toThrow(BindError);
    expect(() =>
      bind(
        "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id",
      ),
    ).toThrow("RIGHT JOIN is not supported");
  });
});

describe("JOIN — more coverage", () => {
  it("self-join resolves correctly with aliases", () => {
    const plan = bind(
      "SELECT a.name, b.name FROM users a JOIN users b ON a.id = b.id",
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    const col1 = proj.expressions[1] as BoundColumnRefExpression;
    expect(col0.columnName).toBe("name");
    expect(col1.columnName).toBe("name");
    // Different table indices for the two aliases
    expect(col0.binding.tableIndex).not.toBe(col1.binding.tableIndex);
  });

  it("JOIN with compound ON condition (AND)", () => {
    const plan = bind(
      "SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND o.amount > 100",
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    expect(join.conditions.length).toBeGreaterThanOrEqual(2);
  });
});

describe("JOIN condition normalization", () => {
  it("ON with right-table column first normalizes to left=left-child, right=right-child", () => {
    // SQL: ON o.user_id = u.id — parser puts o.user_id (right child) in cond.left
    // After normalization: cond.left should reference left child (users)
    const plan = bind(
      "SELECT * FROM users u JOIN orders o ON o.user_id = u.id",
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    const leftGet = join.children[0] as LogicalGet;
    const rightGet = join.children[1] as LogicalGet;

    expect(join.conditions).toHaveLength(1);
    const condLeft = join.conditions[0].left as BoundColumnRefExpression;
    const condRight = join.conditions[0].right as BoundColumnRefExpression;

    expect(condLeft.binding.tableIndex).toBe(leftGet.tableIndex);
    expect(condRight.binding.tableIndex).toBe(rightGet.tableIndex);
  });

  it("ON with left-table column first keeps order unchanged", () => {
    const plan = bind(
      "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    const leftGet = join.children[0] as LogicalGet;
    const rightGet = join.children[1] as LogicalGet;

    const condLeft = join.conditions[0].left as BoundColumnRefExpression;
    const condRight = join.conditions[0].right as BoundColumnRefExpression;

    expect(condLeft.binding.tableIndex).toBe(leftGet.tableIndex);
    expect(condRight.binding.tableIndex).toBe(rightGet.tableIndex);
  });

  it("normalization flips comparisonType for non-EQUAL conditions", () => {
    // ON o.amount > u.age → normalized to cond.left=u.age LESS cond.right=o.amount
    const plan = bind(
      "SELECT * FROM users u JOIN orders o ON o.amount > u.age",
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    const leftGet = join.children[0] as LogicalGet;

    const condLeft = join.conditions[0].left as BoundColumnRefExpression;
    expect(condLeft.binding.tableIndex).toBe(leftGet.tableIndex);
    expect(join.conditions[0].comparisonType).toBe("LESS");
  });

  it("compound ON (AND) normalizes both conditions", () => {
    const plan = bind(
      "SELECT * FROM users u JOIN orders o ON o.user_id = u.id AND o.amount > u.age",
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    const leftGet = join.children[0] as LogicalGet;
    const rightGet = join.children[1] as LogicalGet;

    for (const cond of join.conditions) {
      const left = cond.left as BoundColumnRefExpression;
      const right = cond.right as BoundColumnRefExpression;
      expect(left.binding.tableIndex).toBe(leftGet.tableIndex);
      expect(right.binding.tableIndex).toBe(rightGet.tableIndex);
    }
  });

  it("self-join with reversed condition normalizes correctly", () => {
    const plan = bind("SELECT * FROM users a JOIN users b ON b.id = a.id");
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    const leftGet = join.children[0] as LogicalGet;
    const rightGet = join.children[1] as LogicalGet;

    const condLeft = join.conditions[0].left as BoundColumnRefExpression;
    const condRight = join.conditions[0].right as BoundColumnRefExpression;
    expect(condLeft.binding.tableIndex).toBe(leftGet.tableIndex);
    expect(condRight.binding.tableIndex).toBe(rightGet.tableIndex);
  });
});

describe("JOIN ON type checking", () => {
  it("JOIN ON with compatible types succeeds", () => {
    const plan = bind(
      "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions.length).toBeGreaterThan(0);
  });

  it("JOIN ON with incompatible types throws BindError", () => {
    expect(() =>
      bind("SELECT * FROM users u JOIN orders o ON u.name = o.amount"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT * FROM users u JOIN orders o ON u.name = o.amount"),
    ).toThrow("Type mismatch");
  });

  it("JOIN ON with numeric promotion succeeds", () => {
    // INTEGER = REAL should promote
    const plan = bind("SELECT * FROM users u JOIN orders o ON u.id = o.amount");
    const proj = plan as LogicalProjection;
    expect(proj.expressions.length).toBeGreaterThan(0);
  });

  it("LEFT JOIN ON with incompatible types throws BindError", () => {
    expect(() =>
      bind("SELECT * FROM users u LEFT JOIN orders o ON u.name = o.amount"),
    ).toThrow(BindError);
  });
});

describe("Aggregate in JOIN ON", () => {
  it("aggregate in ON clause throws BindError", () => {
    expect(() =>
      bind("SELECT * FROM users u JOIN orders o ON COUNT(*) > 1"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT * FROM users u JOIN orders o ON COUNT(*) > 1"),
    ).toThrow("JOIN ON clause");
  });
});
