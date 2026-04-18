import { beforeEach, describe, expect, it } from "vitest";
import { BindError } from "../core/errors.js";
import type {
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundJsonAccessExpression,
  LogicalAggregate,
  LogicalAlterTable,
  LogicalFilter,
  LogicalLimit,
  LogicalMaterializedCTE,
  LogicalOrderBy,
  LogicalProjection,
} from "../types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../types.js";
import type { TableSchema } from "../../store/types.js";
import { createTestContext } from "./test_helpers.js";

let catalog: ReturnType<typeof createTestContext>["catalog"];
let bind: ReturnType<typeof createTestContext>["bind"];

beforeEach(() => {
  const ctx = createTestContext();
  catalog = ctx.catalog;
  bind = ctx.bind;
});

describe("Review fixes", () => {
  it("CTE getColumnBindings does not infinite loop", () => {
    const plan = bind(
      "WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active",
    );
    const cte = plan as LogicalMaterializedCTE;
    // This would stack overflow before the fix
    const bindings = cte.getColumnBindings();
    expect(bindings).toBeDefined();
  });

  it("CTE column bindings match resolved column refs", () => {
    const plan = bind(
      "WITH active AS (SELECT id, name FROM users) SELECT name FROM active",
    );
    const cte = plan as LogicalMaterializedCTE;
    const mainProj = cte.children[1] as LogicalProjection;
    const col = mainProj.expressions[0] as BoundColumnRefExpression;
    // The CTE ref's getColumnBindings should have matching tableIndex
    const cteRef = mainProj.children[0];
    const refBindings = cteRef.getColumnBindings();
    // The column's tableIndex should match one of the CTE ref bindings
    expect(
      refBindings.some((b) => b.tableIndex === col.binding.tableIndex),
    ).toBe(true);
  });

  it("SELECT without FROM has valid children (no null)", () => {
    const plan = bind("SELECT 1, 2, 3");
    const proj = plan as LogicalProjection;
    expect(proj.children[0]).toBeDefined();
    expect(proj.children[0].type).toBe(LogicalOperatorType.LOGICAL_GET);
  });

  it("UNION with mismatched column count throws BindError", () => {
    expect(() =>
      bind("SELECT id FROM users UNION SELECT id, status FROM orders"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT id FROM users UNION SELECT id, status FROM orders"),
    ).toThrow("same number of columns");
  });

  it("UNION with incompatible types throws BindError", () => {
    expect(() =>
      bind("SELECT name FROM users UNION SELECT id FROM orders"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT name FROM users UNION SELECT id FROM orders"),
    ).toThrow("Type mismatch");
  });

  it("aggregate in CASE inside SELECT with GROUP BY reuses aggregateIndex", () => {
    const plan = bind(
      "SELECT CASE WHEN COUNT(*) > 1 THEN 'many' ELSE 'few' END FROM users GROUP BY name",
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    // Only one aggregate should be collected
    expect(agg.expressions).toHaveLength(1);
    expect(agg.expressions[0].functionName).toBe("COUNT");
  });
});

describe("Scope isolation", () => {
  it("UNION right side does not see left side tables", () => {
    // 'name' exists in users but not in orders — right side should fail
    expect(() =>
      bind("SELECT name FROM users UNION SELECT name FROM orders"),
    ).toThrow(BindError);
  });

  it("FROM subquery does not see outer tables (no lateral join)", () => {
    // u.id is from outer scope — subquery in FROM should not see it
    expect(() =>
      bind(
        "SELECT * FROM users u JOIN (SELECT * FROM orders WHERE user_id = u.id) sub ON u.id = sub.user_id",
      ),
    ).toThrow(BindError);
  });

  it("UNION right side CTE references still work", () => {
    const plan = bind(
      "WITH cte AS (SELECT id, name FROM users) SELECT id, name FROM cte UNION ALL SELECT id, name FROM cte",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
  });
});

describe("ORDER BY — additional", () => {
  it("ORDER BY DESC", () => {
    const plan = bind("SELECT name FROM users ORDER BY name DESC");
    let node: LogicalOperator = plan;
    while (
      node.type !== LogicalOperatorType.LOGICAL_ORDER_BY &&
      node.children.length > 0
    ) {
      node = node.children[0];
    }
    const orderBy = node as LogicalOrderBy;
    expect(orderBy.orders[0].orderType).toBe("DESCENDING");
  });

  it("multiple ORDER BY columns", () => {
    const plan = bind("SELECT * FROM users ORDER BY name ASC, age DESC");
    let node: LogicalOperator = plan;
    while (
      node.type !== LogicalOperatorType.LOGICAL_ORDER_BY &&
      node.children.length > 0
    ) {
      node = node.children[0];
    }
    const orderBy = node as LogicalOrderBy;
    expect(orderBy.orders).toHaveLength(2);
    expect(orderBy.orders[0].orderType).toBe("ASCENDING");
    expect(orderBy.orders[1].orderType).toBe("DESCENDING");
  });

  it("ORDER BY expression is rewritten to projection output binding", () => {
    const plan = bind("SELECT name, age FROM users ORDER BY age ASC");
    // Plan: ORDER BY → PROJECTION → GET
    const orderBy = plan as LogicalOrderBy;
    expect(orderBy.type).toBe(LogicalOperatorType.LOGICAL_ORDER_BY);
    const proj = orderBy.children[0] as LogicalProjection;
    expect(proj.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);

    // ORDER BY expression should reference projection output, not original table
    const orderExpr = orderBy.orders[0].expression as BoundColumnRefExpression;
    expect(orderExpr.expressionClass).toBe(
      BoundExpressionClass.BOUND_COLUMN_REF,
    );
    const projBindings = proj.getColumnBindings();
    // age is the 2nd select list item → projBindings[1]
    expect(orderExpr.binding.tableIndex).toBe(projBindings[1].tableIndex);
    expect(orderExpr.binding.columnIndex).toBe(projBindings[1].columnIndex);
  });

  it("ORDER BY column not in select list extends projection and adds trim projection", () => {
    const plan = bind("SELECT name FROM users ORDER BY age ASC");
    // Plan: TRIM_PROJECTION → ORDER BY → EXTENDED_PROJECTION → GET
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    const trimProj = plan as LogicalProjection;
    // Trim projection outputs only 1 column (name)
    expect(trimProj.expressions).toHaveLength(1);

    const orderBy = trimProj.children[0] as LogicalOrderBy;
    expect(orderBy.type).toBe(LogicalOperatorType.LOGICAL_ORDER_BY);

    const extendedProj = orderBy.children[0] as LogicalProjection;
    expect(extendedProj.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    // Extended projection has 2 columns (name + age for sort)
    expect(extendedProj.expressions).toHaveLength(2);

    // ORDER BY expression references the extended projection's 2nd column
    const orderExpr = orderBy.orders[0].expression as BoundColumnRefExpression;
    const extBindings = extendedProj.getColumnBindings();
    expect(orderExpr.binding.tableIndex).toBe(extBindings[1].tableIndex);
    expect(orderExpr.binding.columnIndex).toBe(extBindings[1].columnIndex);
  });

  it("ORDER BY with GROUP BY uses aggregate-aware bindings", () => {
    const plan = bind(
      "SELECT name, SUM(amount) FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY name ORDER BY SUM(amount) DESC",
    );
    // Find ORDER BY
    let node: LogicalOperator = plan;
    while (
      node.type !== LogicalOperatorType.LOGICAL_ORDER_BY &&
      node.children.length > 0
    ) {
      node = node.children[0];
    }
    const orderBy = node as LogicalOrderBy;
    expect(orderBy.type).toBe(LogicalOperatorType.LOGICAL_ORDER_BY);

    // ORDER BY expression should be a projection-output column ref
    const orderExpr = orderBy.orders[0].expression as BoundColumnRefExpression;
    expect(orderExpr.expressionClass).toBe(
      BoundExpressionClass.BOUND_COLUMN_REF,
    );

    // It should reference the projection's output binding for the 2nd column (SUM)
    const proj = orderBy.children[0] as LogicalProjection;
    const projBindings = proj.getColumnBindings();
    expect(orderExpr.binding.tableIndex).toBe(projBindings[1].tableIndex);
    expect(orderExpr.binding.columnIndex).toBe(projBindings[1].columnIndex);
  });
});

describe("LIMIT/OFFSET validation", () => {
  it("LIMIT 0 is allowed", () => {
    const plan = bind("SELECT * FROM users LIMIT 0");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_LIMIT);
    expect((plan as LogicalLimit).limitVal).toBe(0);
  });
});

describe("Parameters", () => {
  it("$1 parameter binds to BoundParameterExpression", () => {
    const plan = bind("SELECT * FROM users WHERE age = $1");
    const filter = plan.children[0] as LogicalFilter;
    expect(filter.type).toBe(LogicalOperatorType.LOGICAL_FILTER);
    const cmp = filter.expressions[0] as BoundComparisonExpression;
    expect(cmp.right.expressionClass).toBe(
      BoundExpressionClass.BOUND_PARAMETER,
    );
  });

  it("$2 parameter binds with correct 0-based index", () => {
    const plan = bind("SELECT * FROM users WHERE name = $2");
    const filter = plan.children[0] as LogicalFilter;
    const cmp = filter.expressions[0] as BoundComparisonExpression;
    expect(cmp.right.expressionClass).toBe(
      BoundExpressionClass.BOUND_PARAMETER,
    );
    // $2 maps to 0-based index 1
    expect((cmp.right as any).index).toBe(1);
  });

  it("parameter in INSERT VALUES", () => {
    const plan = bind("INSERT INTO users (id, name) VALUES ($1, $2)");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_INSERT);
  });
});

describe("JSON", () => {
  const jsonSchema: TableSchema = {
    name: "docs",
    columns: [
      { name: "id", type: "INTEGER", nullable: false, primaryKey: true, unique: true, autoIncrement: false, defaultValue: null },
      { name: "data", type: "JSON", nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
      { name: "label", type: "TEXT", nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    ],
  };

  beforeEach(() => {
    catalog.addTable(jsonSchema);
  });

  it("SELECT data.name resolves to BoundJsonAccessExpression", () => {
    const plan = bind("SELECT data.name FROM docs");
    const proj = plan as LogicalProjection;
    expect(proj.expressions[0].expressionClass).toBe(BoundExpressionClass.BOUND_JSON_ACCESS);
    const ja = proj.expressions[0] as BoundJsonAccessExpression;
    expect(ja.child.columnName).toBe("data");
    expect(ja.path).toEqual([{ type: "field", name: "name" }]);
    expect(ja.returnType).toBe("JSON");
  });

  it("SELECT docs.data.name with table alias", () => {
    const plan = bind("SELECT docs.data.name FROM docs");
    const proj = plan as LogicalProjection;
    expect(proj.expressions[0].expressionClass).toBe(BoundExpressionClass.BOUND_JSON_ACCESS);
  });

  it("field access on non-JSON column throws BindError", () => {
    expect(() => bind("SELECT label.x FROM docs")).toThrow(BindError);
  });

  it("ORDER BY JSON column binds successfully", () => {
    const plan = bind("SELECT * FROM docs ORDER BY data");
    expect(plan).toBeDefined();
  });

  it("GROUP BY JSON column binds successfully", () => {
    const plan = bind("SELECT data, COUNT(*) FROM docs GROUP BY data");
    expect(plan).toBeDefined();
  });

  it("DISTINCT with JSON column binds successfully", () => {
    const plan = bind("SELECT DISTINCT data FROM docs");
    expect(plan).toBeDefined();
  });

  it("CREATE INDEX on JSON column throws BindError", () => {
    expect(() => bind("CREATE INDEX idx ON docs(data)")).toThrow(BindError);
  });

  it("arithmetic on JSON throws BindError", () => {
    expect(() => bind("SELECT data + 1 FROM docs")).toThrow(BindError);
  });

  it("LIKE on JSON throws BindError", () => {
    expect(() => bind("SELECT * FROM docs WHERE data LIKE '%x%'")).toThrow(
      /JSON/,
    );
  });

  it("JSON PRIMARY KEY throws BindError", () => {
    expect(() => bind("CREATE TABLE bad (data JSON PRIMARY KEY)")).toThrow(BindError);
  });

  it("JSON UNIQUE throws BindError", () => {
    expect(() => bind("CREATE TABLE bad (id INTEGER, data JSON UNIQUE)")).toThrow(BindError);
  });
});

describe("LIMIT/OFFSET — eval-constant edge cases", () => {
  it("LIMIT with string constant throws BindError", () => {
    expect(() => bind("SELECT * FROM users LIMIT 'abc'")).toThrow(BindError);
    expect(() => bind("SELECT * FROM users LIMIT 'abc'")).toThrow(
      /integer constant/i,
    );
  });

  it("OFFSET with negative value throws BindError", () => {
    expect(() => bind("SELECT * FROM users LIMIT 10 OFFSET -1")).toThrow(
      BindError,
    );
  });
});

describe("type-check — BLOB comparison", () => {
  const blobSchema: TableSchema = {
    name: "blobs",
    columns: [
      { name: "id", type: "INTEGER", nullable: false, primaryKey: true, unique: true, autoIncrement: false, defaultValue: null },
      { name: "data", type: "BLOB", nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    ],
  };

  beforeEach(() => {
    catalog.addTable(blobSchema);
  });

  it("BLOB = BLOB comparison succeeds", () => {
    expect(() =>
      bind("SELECT * FROM blobs a JOIN blobs b ON a.data = b.data"),
    ).not.toThrow();
  });

  it("BLOB compared with TEXT throws type mismatch", () => {
    expect(() =>
      bind("SELECT * FROM blobs WHERE data = 'hello'"),
    ).toThrow(/Type mismatch/);
  });

  it("LIKE on BLOB throws BindError", () => {
    expect(() =>
      bind("SELECT * FROM blobs WHERE data LIKE '%x%'"),
    ).toThrow(/BLOB/);
  });

  it("arithmetic on BLOB throws BindError", () => {
    expect(() =>
      bind("SELECT data + 1 FROM blobs"),
    ).toThrow(/BLOB/);
  });
});

describe("type-check — scalar function type restrictions", () => {
  const blobSchema: TableSchema = {
    name: "blobs",
    columns: [
      { name: "id", type: "INTEGER", nullable: false, primaryKey: true, unique: true, autoIncrement: false, defaultValue: null },
      { name: "data", type: "BLOB", nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    ],
  };

  beforeEach(() => {
    catalog.addTable(blobSchema);
  });

  it("UPPER on BLOB throws BindError", () => {
    expect(() => bind("SELECT UPPER(data) FROM blobs")).toThrow(/BLOB/);
  });

  it("ABS on BLOB throws BindError", () => {
    expect(() => bind("SELECT ABS(data) FROM blobs")).toThrow(/BLOB/);
  });

  it("ROUND on BLOB throws BindError", () => {
    expect(() => bind("SELECT ROUND(data) FROM blobs")).toThrow(/BLOB/);
  });

  it("CONCAT with BLOB throws BindError", () => {
    expect(() =>
      bind("SELECT CONCAT(data, 'x') FROM blobs"),
    ).toThrow(/BLOB/);
  });
});

describe("ALTER TABLE ADD COLUMN — AUTOINCREMENT validation", () => {
  it("AUTOINCREMENT on non-PRIMARY KEY column throws", () => {
    expect(() =>
      bind("ALTER TABLE users ADD COLUMN seq INTEGER AUTOINCREMENT"),
    ).toThrow(/PRIMARY KEY/);
  });

  it("AUTOINCREMENT on non-INTEGER PRIMARY KEY throws", () => {
    expect(() =>
      bind("ALTER TABLE users ADD COLUMN seq TEXT PRIMARY KEY AUTOINCREMENT"),
    ).toThrow(/INTEGER/);
  });

  it("AUTOINCREMENT when table already has one throws", () => {
    const autoSchema: TableSchema = {
      name: "auto_tbl",
      columns: [
        { name: "id", type: "INTEGER", nullable: false, primaryKey: true, unique: true, autoIncrement: true, defaultValue: null },
        { name: "val", type: "TEXT", nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
      ],
    };
    catalog.addTable(autoSchema);
    expect(() =>
      bind("ALTER TABLE auto_tbl ADD COLUMN seq INTEGER PRIMARY KEY AUTOINCREMENT"),
    ).toThrow(/already has an AUTOINCREMENT/);
  });
});

describe("Binder — statement dispatch errors", () => {
  it("BEGIN TRANSACTION throws BindError", () => {
    expect(() => bind("BEGIN")).toThrow(BindError);
    expect(() => bind("BEGIN")).toThrow(/Transaction/);
  });
});

describe("extract-columns — aggregate naming in subquery/CTE", () => {
  it("CTE with COUNT(*) infers column name count_star", () => {
    const plan = bind(
      "WITH agg AS (SELECT COUNT(*) FROM users) SELECT * FROM agg",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const matCte = plan as LogicalMaterializedCTE;
    const proj = matCte.children[1] as LogicalProjection;
    const col = proj.expressions[0] as BoundColumnRefExpression;
    expect(col.columnName).toBe("count_star");
  });

  it("CTE with SUM infers column name sum_0", () => {
    const plan = bind(
      "WITH agg AS (SELECT SUM(age) FROM users) SELECT * FROM agg",
    );
    const matCte = plan as LogicalMaterializedCTE;
    const proj = matCte.children[1] as LogicalProjection;
    const col = proj.expressions[0] as BoundColumnRefExpression;
    expect(col.columnName).toBe("sum_0");
  });
});
