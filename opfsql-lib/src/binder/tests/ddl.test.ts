import { beforeEach, describe, expect, it } from "vitest";
import { BindError } from "../core/errors.js";
import type {
  LogicalAlterTable,
  LogicalCreateIndex,
  LogicalCreateTable,
  LogicalDrop,
} from "../types.js";
import { LogicalOperatorType } from "../types.js";
import type { TableSchema } from "../../store/types.js";
import { createTestContext } from "./test_helpers.js";

let catalog: ReturnType<typeof createTestContext>["catalog"];
let bind: ReturnType<typeof createTestContext>["bind"];

beforeEach(() => {
  const ctx = createTestContext();
  catalog = ctx.catalog;
  bind = ctx.bind;
});

describe("DDL", () => {
  it("CREATE TABLE builds LogicalCreateTable with correct schema", () => {
    const plan = bind(
      "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)",
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_CREATE_TABLE);
    const ct = plan as LogicalCreateTable;
    expect(ct.schema.name).toBe("products");
    expect(ct.schema.columns).toHaveLength(3);
    expect(ct.schema.columns[0].name).toBe("id");
    expect(ct.schema.columns[0].type).toBe("INTEGER");
    expect(ct.schema.columns[0].primaryKey).toBe(true);
    expect(ct.schema.columns[1].name).toBe("name");
    expect(ct.schema.columns[1].type).toBe("TEXT");
    expect(ct.schema.columns[1].nullable).toBe(false);
    expect(ct.schema.columns[2].name).toBe("price");
    expect(ct.schema.columns[2].type).toBe("REAL");
    expect(ct.ifNotExists).toBe(false);
  });

  it("CREATE TABLE IF NOT EXISTS sets ifNotExists = true", () => {
    const plan = bind(
      "CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY)",
    );
    const ct = plan as LogicalCreateTable;
    expect(ct.ifNotExists).toBe(true);
  });

  it("DROP TABLE builds LogicalDrop with dropType TABLE", () => {
    const plan = bind("DROP TABLE users");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_DROP);
    const drop = plan as LogicalDrop;
    expect(drop.dropType).toBe("TABLE");
    expect(drop.name).toBe("users");
    expect(drop.ifExists).toBe(false);
  });

  it("DROP TABLE IF EXISTS sets ifExists = true", () => {
    const plan = bind("DROP TABLE IF EXISTS users");
    const drop = plan as LogicalDrop;
    expect(drop.ifExists).toBe(true);
  });

  it("ALTER TABLE ADD COLUMN builds LogicalAlterTable", () => {
    const plan = bind("ALTER TABLE users ADD COLUMN email TEXT");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_ALTER_TABLE);
    const alter = plan as LogicalAlterTable;
    expect(alter.tableName).toBe("users");
    expect(alter.action.type).toBe("ADD_COLUMN");
    if (alter.action.type === "ADD_COLUMN") {
      expect(alter.action.column.name).toBe("email");
      expect(alter.action.column.type).toBe("TEXT");
    }
  });

  it("CREATE INDEX validates table and columns exist", () => {
    const plan = bind("CREATE INDEX idx_name ON users (name)");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_CREATE_INDEX);
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.tableName).toBe("users");
    expect(ci.index.expressions).toEqual([{ type: 'column', name: 'name', returnType: 'TEXT' }]);
  });

  it("CREATE INDEX on non-existent table throws BindError", () => {
    expect(() => bind("CREATE INDEX idx ON missing_table (col)")).toThrow(
      BindError,
    );
  });

  it("CREATE INDEX on non-existent column throws BindError", () => {
    expect(() => bind("CREATE INDEX idx ON users (missing_col)")).toThrow(
      BindError,
    );
  });
});

describe("DDL — additional", () => {
  it("ALTER TABLE DROP COLUMN", () => {
    const plan = bind("ALTER TABLE users DROP COLUMN age");
    const alter = plan as LogicalAlterTable;
    expect(alter.action.type).toBe("DROP_COLUMN");
    if (alter.action.type === "DROP_COLUMN") {
      expect(alter.action.columnName).toBe("age");
    }
  });

  it("CREATE TABLE with DEFAULT value", () => {
    const plan = bind(
      "CREATE TABLE t (id INTEGER, name TEXT DEFAULT 'unknown')",
    );
    const ct = plan as LogicalCreateTable;
    expect(ct.schema.columns[1].defaultValue).toBe("unknown");
  });

  it("CREATE TABLE with UNIQUE constraint", () => {
    const plan = bind("CREATE TABLE t (id INTEGER, email TEXT UNIQUE)");
    const ct = plan as LogicalCreateTable;
    expect(ct.schema.columns[1].unique).toBe(true);
  });

  it("CREATE TABLE with AUTOINCREMENT maps to autoIncrement: true", () => {
    const plan = bind(
      "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
    );
    const ct = plan as LogicalCreateTable;
    expect(ct.schema.columns[0].autoIncrement).toBe(true);
    expect(ct.schema.columns[0].primaryKey).toBe(true);
    expect(ct.schema.columns[1].autoIncrement).toBe(false);
  });

  it("AUTOINCREMENT on non-INTEGER column throws BindError", () => {
    expect(() =>
      bind("CREATE TABLE t (id TEXT PRIMARY KEY AUTOINCREMENT)"),
    ).toThrow(BindError);
  });

  it("AUTOINCREMENT on non-PRIMARY KEY column throws BindError", () => {
    expect(() =>
      bind("CREATE TABLE t (id INTEGER AUTOINCREMENT, name TEXT PRIMARY KEY)"),
    ).toThrow(BindError);
  });

  it("CREATE UNIQUE INDEX", () => {
    const plan = bind("CREATE UNIQUE INDEX idx_email ON users (name)");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.unique).toBe(true);
  });

  it("DROP INDEX", () => {
    const plan = bind("DROP INDEX idx_name");
    const drop = plan as LogicalDrop;
    expect(drop.dropType).toBe("INDEX");
    expect(drop.name).toBe("idx_name");
  });

  it("DROP TABLE IF EXISTS", () => {
    const plan = bind("DROP TABLE IF EXISTS nonexistent");
    const drop = plan as LogicalDrop;
    expect(drop.ifExists).toBe(true);
    expect(drop.name).toBe("nonexistent");
  });
});

describe("CREATE INDEX — reserved prefix", () => {
  it("index name starting with __pk_ throws BindError", () => {
    expect(() => bind("CREATE INDEX __pk_users ON users (name)")).toThrow(BindError);
    expect(() => bind("CREATE INDEX __pk_users ON users (name)")).toThrow(/__pk_/);
  });

  it("index name starting with __PK_ (case-insensitive) throws BindError", () => {
    expect(() => bind("CREATE INDEX __PK_test ON users (name)")).toThrow(BindError);
  });

  it("index name not starting with __pk_ succeeds", () => {
    const plan = bind("CREATE INDEX idx_users_name ON users (name)");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_CREATE_INDEX);
  });
});

describe("ALTER TABLE DROP COLUMN — index reference check", () => {
  it("DROP COLUMN referenced by index throws BindError", () => {
    catalog.addIndex({
      name: "idx_name",
      tableName: "users",
      expressions: [{ type: 'column', name: 'name', returnType: 'TEXT' }],
      unique: false,
    });
    expect(() => bind("ALTER TABLE users DROP COLUMN name")).toThrow(BindError);
    expect(() => bind("ALTER TABLE users DROP COLUMN name")).toThrow(/referenced by index/);
  });

  it("DROP COLUMN referenced by composite index throws BindError", () => {
    catalog.addIndex({
      name: "idx_name_age",
      tableName: "users",
      expressions: [{ type: 'column', name: 'name', returnType: 'TEXT' }, { type: 'column', name: 'age', returnType: 'INTEGER' }],
      unique: false,
    });
    expect(() => bind("ALTER TABLE users DROP COLUMN age")).toThrow(BindError);
  });

  it("DROP COLUMN not referenced by any index succeeds", () => {
    const plan = bind("ALTER TABLE users DROP COLUMN age");
    const alter = plan as LogicalAlterTable;
    expect(alter.action.type).toBe("DROP_COLUMN");
  });

  it("DROP COLUMN case-insensitive match with index column", () => {
    catalog.addIndex({
      name: "idx_name",
      tableName: "users",
      expressions: [{ type: 'column', name: 'Name', returnType: 'TEXT' }],
      unique: false,
    });
    expect(() => bind("ALTER TABLE users DROP COLUMN name")).toThrow(BindError);
  });
});

describe("BLOB column restrictions", () => {
  it("BLOB PRIMARY KEY throws BindError", () => {
    expect(() => bind("CREATE TABLE bad (data BLOB PRIMARY KEY)")).toThrow(BindError);
  });

  it("BLOB UNIQUE throws BindError", () => {
    expect(() => bind("CREATE TABLE bad (id INTEGER, data BLOB UNIQUE)")).toThrow(BindError);
  });

  it("CREATE INDEX on BLOB column throws BindError", () => {
    catalog.addTable({
      name: "blobs",
      columns: [
        { name: "id", type: "INTEGER", nullable: false, primaryKey: true, unique: true, autoIncrement: false, defaultValue: null },
        { name: "data", type: "BLOB", nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
      ],
    });
    expect(() => bind("CREATE INDEX idx ON blobs(data)")).toThrow(BindError);
  });
});

describe("CREATE INDEX — expression-based", () => {
  const jsonSchema: TableSchema = {
    name: "docs",
    columns: [
      { name: "id", type: "INTEGER", nullable: false, primaryKey: true, unique: true, autoIncrement: false, defaultValue: null },
      { name: "data", type: "JSON", nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
      { name: "label", type: "TEXT", nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
      { name: "score", type: "INTEGER", nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    ],
  };

  beforeEach(() => {
    catalog.addTable(jsonSchema);
  });

  // --- JSON path expressions ---

  it("JSON path expression (data.name) is allowed in index", () => {
    const plan = bind("CREATE INDEX idx ON docs (data.name)");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions).toHaveLength(1);
    expect(ci.index.expressions[0].type).toBe("json_access");
  });

  it("nested JSON path (data.nested.field) is allowed", () => {
    const plan = bind("CREATE INDEX idx ON docs (data.nested.field)");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions[0].type).toBe("json_access");
    expect((ci.index.expressions[0] as any).path).toEqual([
      { type: "field", name: "nested" },
      { type: "field", name: "field" },
    ]);
  });

  it("bare JSON column (data) is rejected", () => {
    expect(() => bind("CREATE INDEX idx ON docs (data)")).toThrow(BindError);
    expect(() => bind("CREATE INDEX idx ON docs (data)")).toThrow(/JSON column directly/);
  });

  // --- Function expressions ---

  it("MD5(label) function expression is allowed", () => {
    const plan = bind("CREATE INDEX idx ON docs (MD5(label))");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions[0].type).toBe("function");
    expect((ci.index.expressions[0] as any).name).toBe("MD5");
  });

  it("MD5(data.name) — function wrapping JSON path is allowed", () => {
    const plan = bind("CREATE INDEX idx ON docs (MD5(data.name))");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions[0].type).toBe("function");
    const fnExpr = ci.index.expressions[0] as any;
    expect(fnExpr.args[0].type).toBe("json_access");
  });

  // --- CAST expressions ---

  it("CAST(score AS TEXT) is allowed", () => {
    const plan = bind("CREATE INDEX idx ON docs (CAST(score AS TEXT))");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions[0].type).toBe("cast");
  });

  it("CAST(data.name AS TEXT) — CAST wrapping JSON path is allowed", () => {
    const plan = bind("CREATE INDEX idx ON docs (CAST(data.name AS TEXT))");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions[0].type).toBe("cast");
    expect((ci.index.expressions[0] as any).child.type).toBe("json_access");
  });

  // --- Operator expressions ---

  it("arithmetic expression (score + id) is allowed", () => {
    const plan = bind("CREATE INDEX idx ON docs (score + id)");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions[0].type).toBe("operator");
  });

  // --- Nested expressions ---

  it("nested MD5(CAST(score AS TEXT)) is allowed", () => {
    const plan = bind("CREATE INDEX idx ON docs (MD5(CAST(score AS TEXT)))");
    const ci = plan as LogicalCreateIndex;
    const expr = ci.index.expressions[0] as any;
    expect(expr.type).toBe("function");
    expect(expr.args[0].type).toBe("cast");
  });

  // --- Composite expression indexes ---

  it("composite (data.name, score) is allowed", () => {
    const plan = bind("CREATE INDEX idx ON docs (data.name, score)");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions).toHaveLength(2);
    expect(ci.index.expressions[0].type).toBe("json_access");
    expect(ci.index.expressions[1].type).toBe("column");
  });

  it("composite with mixed types (data.name, MD5(label), score + id)", () => {
    const plan = bind("CREATE INDEX idx ON docs (data.name, MD5(label), score + id)");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions).toHaveLength(3);
    expect(ci.index.expressions[0].type).toBe("json_access");
    expect(ci.index.expressions[1].type).toBe("function");
    expect(ci.index.expressions[2].type).toBe("operator");
  });

  // --- UNIQUE + expression ---

  it("UNIQUE expression index is allowed", () => {
    const plan = bind("CREATE UNIQUE INDEX idx ON docs (data.name)");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.unique).toBe(true);
    expect(ci.index.expressions[0].type).toBe("json_access");
  });

  // --- Validation: aggregates rejected ---

  it("aggregate in index expression is rejected", () => {
    expect(() => bind("CREATE INDEX idx ON docs (COUNT(label))")).toThrow(BindError);
    expect(() => bind("CREATE INDEX idx ON docs (COUNT(label))")).toThrow(/aggregate/);
  });

  // --- Validation: non-existent column rejected ---

  it("non-existent column in expression is rejected", () => {
    expect(() => bind("CREATE INDEX idx ON docs (nonexistent)")).toThrow(BindError);
  });

  // --- returnType preserved in IndexExpression ---

  it("column expression preserves returnType", () => {
    const plan = bind("CREATE INDEX idx ON docs (score)");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions[0].returnType).toBe("INTEGER");
  });

  it("function expression preserves returnType (MD5 → TEXT)", () => {
    const plan = bind("CREATE INDEX idx ON docs (MD5(label))");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions[0].returnType).toBe("TEXT");
  });

  it("JSON path expression preserves returnType (JSON)", () => {
    const plan = bind("CREATE INDEX idx ON docs (data.name)");
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.expressions[0].returnType).toBe("JSON");
  });
});
