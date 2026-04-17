import { beforeEach, describe, expect, it } from "vitest";
import { BindError } from "../core/errors.js";
import type {
  BoundAggregateExpression,
  BoundColumnRefExpression,
  LogicalAggregate,
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

describe("Aggregates", () => {
  it("SUM returns REAL", () => {
    const plan = bind("SELECT SUM(age) FROM users");
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions[0].returnType).toBe("REAL");
    expect(agg.expressions[0].functionName).toBe("SUM");
  });

  it("AVG returns REAL", () => {
    const plan = bind("SELECT AVG(age) FROM users");
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions[0].returnType).toBe("REAL");
  });

  it("MIN preserves column type", () => {
    const plan = bind("SELECT MIN(age) FROM users");
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions[0].returnType).toBe("INTEGER");
  });

  it("MAX preserves column type", () => {
    const plan = bind("SELECT MAX(name) FROM users");
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions[0].returnType).toBe("TEXT");
  });

  it("GROUP BY with aggregate", () => {
    const plan = bind(
      "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    expect(agg.groups).toHaveLength(1);
    expect(agg.expressions).toHaveLength(1);
    expect(agg.expressions[0].functionName).toBe("COUNT");
  });

  it("HAVING clause binds correctly with aggregate", () => {
    const plan = bind(
      "SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1",
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    expect(agg.havingExpression).not.toBeNull();
    // The aggregate in HAVING should reuse the same aggregateIndex from SELECT
    expect(agg.expressions).toHaveLength(1); // only one COUNT(*)
  });

  it("COUNT(DISTINCT col) sets distinct = true", () => {
    const plan = bind("SELECT COUNT(DISTINCT name) FROM users");
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions[0].distinct).toBe(true);
    expect(agg.expressions[0].functionName).toBe("COUNT");
  });

  it("GROUP BY rewrites column bindings to groupIndex", () => {
    const plan = bind("SELECT name, COUNT(*) FROM users GROUP BY name");
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;

    // Projection's first expression (name) should reference groupIndex, not scan tableIndex
    const nameRef = proj.expressions[0] as BoundColumnRefExpression;
    expect(nameRef.binding.tableIndex).toBe(agg.groupIndex);
    expect(nameRef.binding.columnIndex).toBe(0);
  });

  it("GROUP BY with multiple groups rewrites all bindings", () => {
    const plan = bind(
      "SELECT name, age, COUNT(*) FROM users GROUP BY name, age",
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;

    const nameRef = proj.expressions[0] as BoundColumnRefExpression;
    expect(nameRef.binding.tableIndex).toBe(agg.groupIndex);
    expect(nameRef.binding.columnIndex).toBe(0);

    const ageRef = proj.expressions[1] as BoundColumnRefExpression;
    expect(ageRef.binding.tableIndex).toBe(agg.groupIndex);
    expect(ageRef.binding.columnIndex).toBe(1);
  });

  it("GROUP BY with multiple aggregates binds correctly", () => {
    const plan = bind(
      "SELECT name, COUNT(*), AVG(age) FROM users GROUP BY name",
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;

    expect(agg.groups).toHaveLength(1);
    expect(agg.expressions).toHaveLength(2);

    const nameRef = proj.expressions[0] as BoundColumnRefExpression;
    expect(nameRef.binding.tableIndex).toBe(agg.groupIndex);
    expect(nameRef.binding.columnIndex).toBe(0);

    const countRef = proj.expressions[1] as BoundAggregateExpression;
    expect(countRef.functionName).toBe("COUNT");

    const avgRef = proj.expressions[2] as BoundAggregateExpression;
    expect(avgRef.functionName).toBe("AVG");
  });
});

describe("Aggregate deduplication", () => {
  it("duplicate SUM(age+1) is deduplicated to single aggregate", () => {
    const plan = bind("SELECT SUM(age + 1), SUM(age + 1) FROM users");
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions).toHaveLength(1);
    expect(agg.expressions[0].functionName).toBe("SUM");
  });

  it("different aggregates are not deduplicated", () => {
    const plan = bind("SELECT SUM(age), AVG(age) FROM users");
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions).toHaveLength(2);
  });
});

describe("Aggregate inside function", () => {
  it("UPPER(COUNT(*)) correctly links to pre-collected aggregate", () => {
    const plan = bind(
      "SELECT UPPER(CAST(COUNT(*) AS TEXT)) FROM users GROUP BY name",
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    // Only one aggregate should be collected
    expect(agg.expressions).toHaveLength(1);
    expect(agg.expressions[0].functionName).toBe("COUNT");
  });
});

describe("Aggregate in GROUP BY", () => {
  it("COUNT(*) in GROUP BY throws BindError", () => {
    expect(() => bind("SELECT name FROM users GROUP BY COUNT(*)")).toThrow(
      BindError,
    );
    expect(() => bind("SELECT name FROM users GROUP BY COUNT(*)")).toThrow(
      "GROUP BY clause",
    );
  });

  it("SUM in GROUP BY throws BindError", () => {
    expect(() => bind("SELECT name FROM users GROUP BY SUM(age)")).toThrow(
      BindError,
    );
  });

  it("nested aggregate in GROUP BY expression throws BindError", () => {
    expect(() =>
      bind("SELECT name FROM users GROUP BY age + COUNT(*)"),
    ).toThrow(BindError);
  });
});

describe("Nested aggregates", () => {
  it("SUM(COUNT(*)) throws BindError", () => {
    expect(() => bind("SELECT SUM(COUNT(*)) FROM users GROUP BY name")).toThrow(
      BindError,
    );
    expect(() => bind("SELECT SUM(COUNT(*)) FROM users GROUP BY name")).toThrow(
      "Nested aggregate",
    );
  });

  it("AVG(MAX(age)) throws BindError", () => {
    expect(() => bind("SELECT AVG(MAX(age)) FROM users GROUP BY name")).toThrow(
      BindError,
    );
  });

  it("COUNT(SUM(age)) throws BindError", () => {
    expect(() =>
      bind("SELECT COUNT(SUM(age)) FROM users GROUP BY name"),
    ).toThrow(BindError);
  });

  it("simple aggregate (no nesting) still works", () => {
    const plan = bind("SELECT name, COUNT(*) FROM users GROUP BY name");
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
  });
});

describe("Non-aggregated column not in GROUP BY", () => {
  it("SELECT column not in GROUP BY throws BindError", () => {
    expect(() => bind("SELECT name, age FROM users GROUP BY name")).toThrow(
      BindError,
    );
    expect(() => bind("SELECT name, age FROM users GROUP BY name")).toThrow(
      "must appear in the GROUP BY",
    );
  });

  it("SELECT column in GROUP BY succeeds", () => {
    const plan = bind("SELECT name FROM users GROUP BY name");
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(1);
  });

  it("SELECT column in GROUP BY with aggregate succeeds", () => {
    const plan = bind("SELECT name, COUNT(*) FROM users GROUP BY name");
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
  });

  it("implicit GROUP BY (aggregates only, no GROUP BY clause) rejects bare columns", () => {
    expect(() => bind("SELECT name, COUNT(*) FROM users")).toThrow(BindError);
    expect(() => bind("SELECT name, COUNT(*) FROM users")).toThrow(
      "must appear in the GROUP BY",
    );
  });

  it("implicit GROUP BY with only aggregates succeeds", () => {
    const plan = bind("SELECT COUNT(*), MAX(age) FROM users");
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
  });

  it("SELECT * with GROUP BY throws if not all columns are grouped", () => {
    expect(() => bind("SELECT * FROM users GROUP BY name")).toThrow(BindError);
    expect(() => bind("SELECT * FROM users GROUP BY name")).toThrow(
      "must appear in the GROUP BY",
    );
  });

  it("HAVING referencing non-grouped column throws BindError", () => {
    expect(() =>
      bind("SELECT name, COUNT(*) FROM users GROUP BY name HAVING age > 10"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT name, COUNT(*) FROM users GROUP BY name HAVING age > 10"),
    ).toThrow("must appear in the GROUP BY");
  });

  it("HAVING with aggregate on non-grouped column succeeds", () => {
    const plan = bind(
      "SELECT name, COUNT(*) FROM users GROUP BY name HAVING MAX(age) > 18",
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
  });

  it("expression in SELECT that wraps grouped column succeeds with aggregate", () => {
    const plan = bind("SELECT UPPER(name), COUNT(*) FROM users GROUP BY name");
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
  });
});

describe("HAVING — additional", () => {
  it("HAVING without explicit GROUP BY but with aggregate", () => {
    const plan = bind("SELECT COUNT(*) FROM users HAVING COUNT(*) > 1");
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    expect(agg.havingExpression).not.toBeNull();
    expect(agg.groups).toHaveLength(0);
  });
});
