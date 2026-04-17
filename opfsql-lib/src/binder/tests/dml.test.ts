import { beforeEach, describe, expect, it } from "vitest";
import { BindError } from "../core/errors.js";
import type {
  BoundColumnRefExpression,
  LogicalDelete,
  LogicalInsert,
  LogicalUpdate,
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

describe("DML", () => {
  it("INSERT builds LogicalInsert with correct columns", () => {
    const plan = bind("INSERT INTO users (id, name) VALUES (1, 'Alice')");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_INSERT);
    const ins = plan as LogicalInsert;
    expect(ins.tableName).toBe("users");
    expect(ins.columns).toEqual([0, 1]); // indices of id, name
    expect(ins.expressions).toHaveLength(2);
  });

  it("UPDATE builds LogicalUpdate with filter", () => {
    const plan = bind("UPDATE users SET name = 'Bob' WHERE id = 1");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_UPDATE);
    const upd = plan as LogicalUpdate;
    expect(upd.tableName).toBe("users");
    expect(upd.updateColumns).toEqual([1]); // index of name
    expect(upd.children[0].type).toBe(LogicalOperatorType.LOGICAL_FILTER);
  });

  it("DELETE builds LogicalDelete with filter", () => {
    const plan = bind("DELETE FROM users WHERE id = 1");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_DELETE);
    const del = plan as LogicalDelete;
    expect(del.tableName).toBe("users");
    expect(del.children[0].type).toBe(LogicalOperatorType.LOGICAL_FILTER);
  });

  it("INSERT into non-existent table throws BindError", () => {
    expect(() => bind("INSERT INTO missing (id) VALUES (1)")).toThrow(
      BindError,
    );
  });

  it("INSERT with non-existent column throws BindError", () => {
    expect(() => bind("INSERT INTO users (missing_col) VALUES (1)")).toThrow(
      BindError,
    );
  });

  it("INSERT ON CONFLICT DO NOTHING builds onConflict", () => {
    const plan = bind(
      "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING",
    );
    const ins = plan as LogicalInsert;
    expect(ins.onConflict).toBeDefined();
    expect(ins.onConflict!.action).toBe("NOTHING");
    expect(ins.onConflict!.conflictColumns).toEqual([0]); // id is column 0
  });

  it("INSERT ON CONFLICT DO UPDATE SET builds update info", () => {
    const plan = bind(
      "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = excluded.name",
    );
    const ins = plan as LogicalInsert;
    expect(ins.onConflict).toBeDefined();
    expect(ins.onConflict!.action).toBe("UPDATE");
    expect(ins.onConflict!.updateColumns).toEqual([1]); // name is column 1
    expect(ins.onConflict!.updateExpressions).toHaveLength(1);
    expect(ins.onConflict!.excludedTableIndex).toBeGreaterThanOrEqual(0);
  });

  it("INSERT ON CONFLICT DO UPDATE with WHERE", () => {
    const plan = bind(
      "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30) ON CONFLICT (id) DO UPDATE SET age = excluded.age WHERE users.age < excluded.age",
    );
    const ins = plan as LogicalInsert;
    expect(ins.onConflict!.action).toBe("UPDATE");
    expect(ins.onConflict!.whereExpression).not.toBeNull();
  });

  it("INSERT ON CONFLICT with no target uses primary key", () => {
    const plan = bind(
      "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT DO NOTHING",
    );
    const ins = plan as LogicalInsert;
    expect(ins.onConflict!.conflictColumns).toEqual([0]); // id is the PK
  });

  it("ON CONFLICT with non-existent column throws BindError", () => {
    expect(() =>
      bind(
        "INSERT INTO users (id) VALUES (1) ON CONFLICT (missing) DO NOTHING",
      ),
    ).toThrow(BindError);
  });

  it("ON CONFLICT with non-unique column throws BindError", () => {
    expect(() =>
      bind(
        "INSERT INTO users (id, name) VALUES (1, 'x') ON CONFLICT (name) DO NOTHING",
      ),
    ).toThrow(BindError);
  });

  it("excluded references resolve in DO UPDATE expressions", () => {
    const plan = bind(
      "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = excluded.name",
    );
    const ins = plan as LogicalInsert;
    const expr = ins.onConflict!.updateExpressions[0] as BoundColumnRefExpression;
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_COLUMN_REF);
    expect(expr.binding.tableIndex).toBe(ins.onConflict!.excludedTableIndex);
    expect(expr.columnName).toBe("name");
  });
});

describe("DML — additional", () => {
  it("DELETE without WHERE scans all rows", () => {
    const plan = bind("DELETE FROM users");
    const del = plan as LogicalDelete;
    expect(del.children[0].type).toBe(LogicalOperatorType.LOGICAL_GET);
  });

  it("UPDATE without WHERE scans all rows", () => {
    const plan = bind("UPDATE users SET name = 'Bob'");
    const upd = plan as LogicalUpdate;
    expect(upd.children[0].type).toBe(LogicalOperatorType.LOGICAL_GET);
  });

  it("UPDATE with non-existent column throws BindError", () => {
    expect(() => bind("UPDATE users SET missing_col = 1")).toThrow(BindError);
  });
});

describe("INSERT SELECT", () => {
  it("INSERT INTO ... SELECT produces LogicalInsert with child plan", () => {
    const plan = bind(
      "INSERT INTO orders (id, user_id, amount, status) SELECT id, id, age, name FROM users",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_INSERT);
    const ins = plan as LogicalInsert;
    expect(ins.tableName).toBe("orders");
    expect(ins.children).toHaveLength(1);
    expect(ins.children[0].type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    expect(ins.expressions).toHaveLength(0); // no VALUES expressions
  });
});

describe("INSERT validation", () => {
  it("INSERT VALUES with too many values throws BindError", () => {
    expect(() =>
      bind("INSERT INTO users (id, name) VALUES (1, 'Alice', 42)"),
    ).toThrow(BindError);
    expect(() =>
      bind("INSERT INTO users (id, name) VALUES (1, 'Alice', 42)"),
    ).toThrow("column count mismatch");
  });

  it("INSERT VALUES with too few values throws BindError", () => {
    expect(() =>
      bind("INSERT INTO users (id, name, age) VALUES (1, 'Alice')"),
    ).toThrow(BindError);
    expect(() =>
      bind("INSERT INTO users (id, name, age) VALUES (1, 'Alice')"),
    ).toThrow("column count mismatch");
  });

  it("INSERT SELECT with column count mismatch throws BindError", () => {
    expect(() =>
      bind("INSERT INTO users (id, name) SELECT id, name, age FROM users"),
    ).toThrow(BindError);
    expect(() =>
      bind("INSERT INTO users (id, name) SELECT id, name, age FROM users"),
    ).toThrow("column count mismatch");
  });

  it("INSERT without column list uses all columns", () => {
    const plan = bind("INSERT INTO users VALUES (1, 'Alice', 25, true)");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_INSERT);
    const ins = plan as LogicalInsert;
    expect(ins.columns).toEqual([0, 1, 2, 3]);
    expect(ins.expressions).toHaveLength(4);
  });
});

describe("INSERT duplicate columns", () => {
  it("INSERT with duplicate column names throws BindError", () => {
    expect(() =>
      bind("INSERT INTO users (name, name) VALUES ('a', 'b')"),
    ).toThrow(BindError);
    expect(() =>
      bind("INSERT INTO users (name, name) VALUES ('a', 'b')"),
    ).toThrow("Duplicate column");
  });
});
