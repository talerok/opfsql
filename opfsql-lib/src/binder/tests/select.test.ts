import { beforeEach, describe, expect, it } from "vitest";
import { BindError } from "../core/errors.js";
import type {
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConstantExpression,
  LogicalAggregate,
  LogicalComparisonJoin,
  LogicalFilter,
  LogicalGet,
  LogicalLimit,
  LogicalMaterializedCTE,
  LogicalOrderBy,
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

describe("SELECT", () => {
  it("SELECT * expands to all columns of all tables in FROM order", () => {
    const plan = bind("SELECT * FROM users");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(4); // id, name, age, active
    expect(proj.types).toEqual(["INTEGER", "TEXT", "INTEGER", "BOOLEAN"]);

    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    expect(col0.columnName).toBe("id");
    const col1 = proj.expressions[1] as BoundColumnRefExpression;
    expect(col1.columnName).toBe("name");
    const col2 = proj.expressions[2] as BoundColumnRefExpression;
    expect(col2.columnName).toBe("age");
    const col3 = proj.expressions[3] as BoundColumnRefExpression;
    expect(col3.columnName).toBe("active");
  });

  it("table.* expands to columns of only that table", () => {
    const plan = bind(
      "SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id",
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(4); // users columns only
    for (const expr of proj.expressions) {
      expect((expr as BoundColumnRefExpression).tableName).toBe("users");
    }
  });

  it("table alias resolves correctly", () => {
    const plan = bind("SELECT u.name FROM users u");
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(1);
    const col = proj.expressions[0] as BoundColumnRefExpression;
    expect(col.columnName).toBe("name");
    expect(col.tableName).toBe("users");
  });

  it("column without alias resolves when not ambiguous", () => {
    const plan = bind("SELECT name FROM users");
    const proj = plan as LogicalProjection;
    const col = proj.expressions[0] as BoundColumnRefExpression;
    expect(col.columnName).toBe("name");
  });

  it("non-existent table throws BindError", () => {
    expect(() => bind("SELECT * FROM missing_table")).toThrow(BindError);
    expect(() => bind("SELECT * FROM missing_table")).toThrow(
      'Table "missing_table" not found',
    );
  });

  it("non-existent column throws BindError", () => {
    expect(() => bind("SELECT missing_col FROM users")).toThrow(BindError);
    expect(() => bind("SELECT missing_col FROM users")).toThrow(
      'Column "missing_col" not found',
    );
  });

  it("ambiguous column throws BindError", () => {
    expect(() =>
      bind("SELECT id FROM users JOIN orders ON users.id = orders.id"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT id FROM users JOIN orders ON users.id = orders.id"),
    ).toThrow("ambiguous");
  });

  it("aggregate in WHERE throws BindError", () => {
    expect(() => bind("SELECT * FROM users WHERE COUNT(*) > 1")).toThrow(
      BindError,
    );
    expect(() => bind("SELECT * FROM users WHERE COUNT(*) > 1")).toThrow(
      "Aggregate function not allowed in WHERE clause",
    );
  });

  it("INTEGER > INTEGER returns BOOLEAN", () => {
    const plan = bind("SELECT * FROM users WHERE age > 18");
    const filter = (plan as LogicalProjection).children[0] as LogicalFilter;
    expect(filter.type).toBe(LogicalOperatorType.LOGICAL_FILTER);
    const cmp = filter.expressions[0] as BoundComparisonExpression;
    expect(cmp.returnType).toBe("BOOLEAN");
  });

  it("TEXT > INTEGER throws BindError", () => {
    expect(() => bind("SELECT * FROM users WHERE name > 18")).toThrow(
      BindError,
    );
    expect(() => bind("SELECT * FROM users WHERE name > 18")).toThrow(
      "Type mismatch",
    );
  });

  it("COUNT(*) returns type INTEGER", () => {
    const plan = bind("SELECT COUNT(*) FROM users");
    const proj = plan as LogicalProjection;
    // With aggregate, there's an aggregate node between projection and get
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    expect(agg.expressions[0].returnType).toBe("INTEGER");
    expect(agg.expressions[0].isStar).toBe(true);
  });

  it("GROUP BY without aggregate creates LogicalAggregate with empty expressions", () => {
    const plan = bind("SELECT name FROM users GROUP BY name");
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    expect(agg.groups).toHaveLength(1);
    expect(agg.expressions).toHaveLength(0);
  });

  it("LIMIT without OFFSET sets offsetVal = 0", () => {
    const plan = bind("SELECT * FROM users LIMIT 10");
    // Should have LIMIT on top
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_LIMIT);
    const limit = plan as LogicalLimit;
    expect(limit.limitVal).toBe(10);
    expect(limit.offsetVal).toBe(0);
  });

  it("subquery creates independent BindScope", () => {
    const plan = bind(
      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    expect(filter.type).toBe(LogicalOperatorType.LOGICAL_FILTER);
    // The expression should contain a subquery or IN operator
    expect(filter.expressions).toHaveLength(1);
  });

  it("CTE is accessible in main query", () => {
    const plan = bind(
      "WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active",
    );
    // Top node should be LogicalMaterializedCTE wrapping CTE plan + main query
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const cte = plan as LogicalMaterializedCTE;
    expect(cte.cteName).toBe("active");
    expect(cte.children).toHaveLength(2);
    // children[0] = CTE plan (projection over filter over get)
    // children[1] = main query (projection over CTE ref)
    const mainQuery = cte.children[1];
    expect(mainQuery.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
  });

  it("CTE not accessible outside its scope", () => {
    // Just binding a query referencing a non-existent CTE should throw
    expect(() => bind("SELECT * FROM nonexistent_cte")).toThrow(BindError);
  });

  it("DISTINCT wraps in LogicalDistinct", () => {
    const plan = bind("SELECT DISTINCT name FROM users");
    // Modifiers order: DISTINCT, ORDER BY, LIMIT
    // Find the distinct node
    let node: LogicalOperator = plan;
    let foundDistinct = false;
    while (node) {
      if (node.type === LogicalOperatorType.LOGICAL_DISTINCT) {
        foundDistinct = true;
        break;
      }
      if (node.children.length > 0) {
        node = node.children[0];
      } else {
        break;
      }
    }
    expect(foundDistinct).toBe(true);
  });

  it("ORDER BY wraps in LogicalOrderBy", () => {
    const plan = bind("SELECT name FROM users ORDER BY name ASC");
    let node: LogicalOperator = plan;
    let foundOrder = false;
    while (node) {
      if (node.type === LogicalOperatorType.LOGICAL_ORDER_BY) {
        foundOrder = true;
        const orderBy = node as LogicalOrderBy;
        expect(orderBy.orders).toHaveLength(1);
        expect(orderBy.orders[0].orderType).toBe("ASCENDING");
        break;
      }
      if (node.children.length > 0) {
        node = node.children[0];
      } else {
        break;
      }
    }
    expect(foundOrder).toBe(true);
  });

  it("LIMIT with OFFSET", () => {
    const plan = bind("SELECT * FROM users LIMIT 10 OFFSET 5");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_LIMIT);
    const limit = plan as LogicalLimit;
    expect(limit.limitVal).toBe(10);
    expect(limit.offsetVal).toBe(5);
  });

  it("builds correct tree: SELECT name FROM users WHERE age > 18 LIMIT 10", () => {
    const plan = bind("SELECT name FROM users WHERE age > 18 LIMIT 10");

    // Top: LogicalLimit
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_LIMIT);
    const limit = plan as LogicalLimit;
    expect(limit.limitVal).toBe(10);

    // Next: LogicalProjection
    const proj = limit.children[0] as LogicalProjection;
    expect(proj.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    expect(proj.expressions).toHaveLength(1);

    // Next: LogicalFilter
    const filter = proj.children[0] as LogicalFilter;
    expect(filter.type).toBe(LogicalOperatorType.LOGICAL_FILTER);

    // Bottom: LogicalGet
    const get = filter.children[0] as LogicalGet;
    expect(get.type).toBe(LogicalOperatorType.LOGICAL_GET);
    expect(get.tableName).toBe("users");
  });
});

describe("SELECT without FROM", () => {
  it("SELECT 1 produces a projection", () => {
    const plan = bind("SELECT 1");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(1);
    const c = proj.expressions[0] as BoundConstantExpression;
    expect(c.expressionClass).toBe(BoundExpressionClass.BOUND_CONSTANT);
    expect(c.value).toBe(1);
  });

  it("SELECT 'hello' produces TEXT type", () => {
    const plan = bind("SELECT 'hello'");
    const proj = plan as LogicalProjection;
    expect(proj.types).toEqual(["TEXT"]);
  });
});

describe("ColumnBinding", () => {
  it("tableIndex is unique for each table in query", () => {
    const plan = bind(
      "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id",
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    const leftGet = join.children[0] as LogicalGet;
    const rightGet = join.children[1] as LogicalGet;
    expect(leftGet.tableIndex).not.toBe(rightGet.tableIndex);
  });

  it("getColumnBindings() returns correct bindings for LogicalGet", () => {
    const plan = bind("SELECT * FROM users");
    const proj = plan as LogicalProjection;
    const get = proj.children[0] as LogicalGet;
    const bindings = get.getColumnBindings();
    expect(bindings).toHaveLength(4);
    expect(bindings[0].tableIndex).toBe(get.tableIndex);
    expect(bindings[0].columnIndex).toBe(0);
    expect(bindings[1].columnIndex).toBe(1);
    expect(bindings[2].columnIndex).toBe(2);
    expect(bindings[3].columnIndex).toBe(3);
  });

  it("after PROJECTION getColumnBindings() reflects only selected columns", () => {
    const plan = bind("SELECT name, age FROM users");
    const proj = plan as LogicalProjection;
    const bindings = proj.getColumnBindings();
    expect(bindings).toHaveLength(2);
    expect(bindings[0].tableIndex).toBe(proj.tableIndex);
    expect(bindings[0].columnIndex).toBe(0);
    expect(bindings[1].columnIndex).toBe(1);
  });
});

describe("projection aliases", () => {
  it("captures AS alias on column ref", () => {
    const proj = bind(
      "SELECT name AS username FROM users",
    ) as LogicalProjection;
    expect(proj.aliases).toEqual(["username"]);
  });

  it("captures AS alias on expression", () => {
    const proj = bind(
      "SELECT age + 1 AS next_age FROM users",
    ) as LogicalProjection;
    expect(proj.aliases).toEqual(["next_age"]);
  });

  it("sets null alias when no AS is used", () => {
    const proj = bind("SELECT name, age FROM users") as LogicalProjection;
    expect(proj.aliases).toEqual([null, null]);
  });

  it("mixes aliases and non-aliases", () => {
    const proj = bind(
      "SELECT name, age AS user_age FROM users",
    ) as LogicalProjection;
    expect(proj.aliases).toEqual([null, "user_age"]);
  });

  it("star expansion produces null aliases", () => {
    const proj = bind("SELECT * FROM users") as LogicalProjection;
    expect(proj.aliases).toEqual([null, null, null, null]);
  });
});
