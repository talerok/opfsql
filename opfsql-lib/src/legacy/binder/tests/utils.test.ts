import { aggregateFunction } from "../../../functions/index.js";
import { describe, it, expect, vi } from "vitest";
import {
  findColumnIndex,
  findColumnIndexOrThrow,
  getPrimaryKeyColumns,
} from "../core/utils/find-column.js";
import {
  evalConstantInt,
  evalDefaultValue,
} from "../core/utils/eval-constant.js";
import {
  findProjection,
  extractColumnsFromPlan,
} from "../core/utils/extract-columns.js";
import { requireTable } from "../core/utils/require-table.js";
import { NotSupportedError, SemanticError } from "../../../errors.js";
import { parse } from "../../../sql/parser/index.js";
import {
  StatementType,
  type ParsedExpression,
} from "../../../sql/parser/types.js";
import type { TableSchema } from "../../../store/types.js";
import type { ColumnDef } from "../../../types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../types.js";
import type { LogicalGet, LogicalProjection, LogicalFilter } from "../types.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const usersSchema: TableSchema = {
  name: "users",
  columns: [
    {
      name: "id",
      type: "INTEGER",
      nullable: false,
      primaryKey: true,
      unique: true,
      autoIncrement: false,
      defaultValue: null,
    },
    {
      name: "name",
      type: "TEXT",
      nullable: false,
      primaryKey: false,
      unique: false,
      autoIncrement: false,
      defaultValue: null,
    },
    {
      name: "age",
      type: "INTEGER",
      nullable: true,
      primaryKey: false,
      unique: false,
      autoIncrement: false,
      defaultValue: null,
    },
  ],
};

const compositeSchema: TableSchema = {
  name: "order_items",
  columns: [
    {
      name: "order_id",
      type: "INTEGER",
      nullable: false,
      primaryKey: true,
      unique: false,
      autoIncrement: false,
      defaultValue: null,
    },
    {
      name: "item_id",
      type: "INTEGER",
      nullable: false,
      primaryKey: true,
      unique: false,
      autoIncrement: false,
      defaultValue: null,
    },
    {
      name: "qty",
      type: "INTEGER",
      nullable: true,
      primaryKey: false,
      unique: false,
      autoIncrement: false,
      defaultValue: null,
    },
  ],
};

// ---------------------------------------------------------------------------
// find-column
// ---------------------------------------------------------------------------

describe("findColumnIndex", () => {
  it("finds column by exact name", () => {
    expect(findColumnIndex(usersSchema, "name")).toBe(1);
  });

  it("case-insensitive lookup", () => {
    expect(findColumnIndex(usersSchema, "NAME")).toBe(1);
    expect(findColumnIndex(usersSchema, "Id")).toBe(0);
  });

  it("returns -1 for missing column", () => {
    expect(findColumnIndex(usersSchema, "email")).toBe(-1);
  });
});

describe("findColumnIndexOrThrow", () => {
  it("returns index for existing column", () => {
    expect(findColumnIndexOrThrow(usersSchema, "age")).toBe(2);
  });

  it("throws BindError for missing column", () => {
    expect(() => findColumnIndexOrThrow(usersSchema, "email")).toThrow(
      BindError,
    );
    expect(() => findColumnIndexOrThrow(usersSchema, "email")).toThrow(
      /not found/,
    );
  });
});

describe("getPrimaryKeyColumns", () => {
  it("returns single PK column index", () => {
    expect(getPrimaryKeyColumns(usersSchema)).toEqual([0]);
  });

  it("returns multiple PK column indexes for composite key", () => {
    expect(getPrimaryKeyColumns(compositeSchema)).toEqual([0, 1]);
  });

  it("returns empty array for table without PK", () => {
    const noPK: TableSchema = {
      name: "logs",
      columns: [
        {
          name: "msg",
          type: "TEXT",
          nullable: true,
          primaryKey: false,
          unique: false,
          autoIncrement: false,
          defaultValue: null,
        },
      ],
    };
    expect(getPrimaryKeyColumns(noPK)).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// eval-constant
// ---------------------------------------------------------------------------

/** The first select list item of `SELECT <sql>`. */
function expressionOf(sql: string): ParsedExpression {
  const [statement] = parse(`SELECT ${sql}`);
  if (statement.type !== StatementType.SELECT_STATEMENT) {
    throw new Error("expected a SELECT");
  }
  const node = statement.body;
  if (node.type !== "SELECT_NODE") throw new Error("expected a SELECT node");
  return node.selectList[0];
}

describe("evalConstantInt", () => {
  it("extracts integer from constant expression", () => {
    expect(evalConstantInt(expressionOf("42"))).toBe(42);
  });

  it("throws for negative integer", () => {
    expect(() => evalConstantInt(expressionOf("-5"))).toThrow(BindError);
  });

  it("throws for non-constant expression", () => {
    expect(() => evalConstantInt(expressionOf("a"))).toThrow(BindError);
  });

  it("throws for string constant", () => {
    expect(() => evalConstantInt(expressionOf("'hello'"))).toThrow(BindError);
  });
});

describe("evalDefaultValue", () => {
  it("extracts value from constant expression", () => {
    expect(evalDefaultValue("c", expressionOf("'hello'"))).toBe("hello");
    expect(evalDefaultValue("c", expressionOf("-1"))).toBe(-1);
    expect(evalDefaultValue("c", expressionOf("NULL"))).toBeNull();
    expect(evalDefaultValue("c", expressionOf("(1)"))).toBe(1);
  });

  it("rejects an expression that is not constant", () => {
    for (const sql of ["a", "$1", "(SELECT 1)", "1 + a", "lower(a)"]) {
      expect(() => evalDefaultValue("c", expressionOf(sql))).toThrow(
        SemanticError,
      );
    }
  });

  it("does not evaluate a constant expression yet", () => {
    for (const sql of ["1 + 2", "lower('A')", "CAST(1 AS TEXT)"]) {
      expect(() => evalDefaultValue("c", expressionOf(sql))).toThrow(
        NotSupportedError,
      );
    }
  });
});

// ---------------------------------------------------------------------------
// extract-columns
// ---------------------------------------------------------------------------

describe("findProjection", () => {
  it("returns projection at root", () => {
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 1,
      children: [
        {
          type: LogicalOperatorType.LOGICAL_GET,
          children: [],
          expressions: [],
          types: [],
          estimatedCardinality: 0,
          columnBindings: [],
        } as any,
      ],
      expressions: [],
      aliases: [],
      types: [],
      estimatedCardinality: 0,
      columnBindings: [],
    };
    expect(findProjection(proj)).toBe(proj);
  });

  it("finds projection in child", () => {
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 1,
      children: [
        {
          type: LogicalOperatorType.LOGICAL_GET,
          children: [],
          expressions: [],
          types: [],
          estimatedCardinality: 0,
          columnBindings: [],
        } as any,
      ],
      expressions: [],
      aliases: [],
      types: [],
      estimatedCardinality: 0,
      columnBindings: [],
    };
    const filter: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [proj],
      expressions: [],
      types: [],
      estimatedCardinality: 0,
      columnBindings: [],
    };
    expect(findProjection(filter)).toBe(proj);
  });

  it("returns null when no projection exists", () => {
    const get = {
      type: LogicalOperatorType.LOGICAL_GET,
      children: [],
      expressions: [],
      types: [],
      estimatedCardinality: 0,
      columnBindings: [],
    };
    expect(findProjection(get as any)).toBeNull();
  });
});

describe("extractColumnsFromPlan", () => {
  it("uses projection aliases", () => {
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 1,
      children: [
        {
          type: LogicalOperatorType.LOGICAL_GET,
          children: [],
          expressions: [],
          types: [],
          estimatedCardinality: 0,
          columnBindings: [],
        } as any,
      ],
      expressions: [
        {
          expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
          binding: { tableIndex: 0, columnIndex: 0 },
          tableName: "t0",
          columnName: "id",
          returnType: "INTEGER",
        },
      ],
      aliases: ["user_id"],
      types: ["INTEGER"],
      estimatedCardinality: 0,
      columnBindings: [],
    };
    const cols = extractColumnsFromPlan(proj, ["INTEGER"]);
    expect(cols[0].name).toBe("user_id");
  });

  it("uses column name when no alias", () => {
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 1,
      children: [
        {
          type: LogicalOperatorType.LOGICAL_GET,
          children: [],
          expressions: [],
          types: [],
          estimatedCardinality: 0,
          columnBindings: [],
        } as any,
      ],
      expressions: [
        {
          expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
          binding: { tableIndex: 0, columnIndex: 0 },
          tableName: "t0",
          columnName: "id",
          returnType: "INTEGER",
        },
      ],
      aliases: [null],
      types: ["INTEGER"],
      estimatedCardinality: 0,
      columnBindings: [],
    };
    const cols = extractColumnsFromPlan(proj, ["INTEGER"]);
    expect(cols[0].name).toBe("id");
  });

  it("generates column0..N when no projection", () => {
    const get = {
      type: LogicalOperatorType.LOGICAL_GET,
      children: [],
      expressions: [],
      types: ["INTEGER", "TEXT"],
      estimatedCardinality: 0,
      columnBindings: [],
    };
    const cols = extractColumnsFromPlan(get as any, ["INTEGER", "TEXT"]);
    expect(cols[0].name).toBe("column0");
    expect(cols[1].name).toBe("column1");
    expect(cols[0].type).toBe("INTEGER");
    expect(cols[1].type).toBe("TEXT");
  });

  it("aggregate expression generates function-based name", () => {
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 1,
      children: [
        {
          type: LogicalOperatorType.LOGICAL_GET,
          children: [],
          expressions: [],
          types: [],
          estimatedCardinality: 0,
          columnBindings: [],
        } as any,
      ],
      expressions: [
        {
          expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
          functionName: "COUNT",
          function: aggregateFunction("COUNT"),
          isStar: true,
          distinct: false,
          aggregateIndex: 0,
          children: [],
          returnType: "INTEGER",
        },
      ],
      aliases: [null],
      types: ["INTEGER"],
      estimatedCardinality: 0,
      columnBindings: [],
    };
    const cols = extractColumnsFromPlan(proj, ["INTEGER"]);
    expect(cols[0].name).toBe("count_star");
  });
});

// ---------------------------------------------------------------------------
// require-table
// ---------------------------------------------------------------------------

describe("requireTable", () => {
  it("returns schema when table exists", () => {
    const ctx = {
      catalog: {
        getTable: vi.fn((name: string) =>
          name === "users" ? usersSchema : undefined,
        ),
      },
    } as unknown as BindContext;
    expect(requireTable(ctx, "users")).toBe(usersSchema);
  });

  it("throws BindError when table not found", () => {
    const ctx = {
      catalog: {
        getTable: vi.fn(() => undefined),
      },
    } as unknown as BindContext;
    expect(() => requireTable(ctx, "nonexistent")).toThrow(BindError);
    expect(() => requireTable(ctx, "nonexistent")).toThrow(/not found/);
  });
});
