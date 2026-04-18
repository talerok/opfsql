import { describe, expect, it } from "vitest";
import { formatExpression, formatPlan } from "../explain.js";
import {
  BoundExpressionClass,
  LogicalOperatorType,
  type BoundExpression,
  type LogicalOperator,
  type LogicalGet,
  type LogicalFilter,
  type LogicalProjection,
  type LogicalComparisonJoin,
  type ColumnBinding,
} from "../../binder/types.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function colRef(table: string, col: string): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
    tableName: table,
    columnName: col,
    binding: { tableIndex: 0, columnIndex: 0 },
    returnType: "INTEGER",
  };
}

function constant(value: string | number | boolean | null): BoundExpression {
  const returnType =
    value === null
      ? "NULL"
      : typeof value === "string"
        ? "TEXT"
        : typeof value === "boolean"
          ? "BOOLEAN"
          : "INTEGER";
  return {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value,
    returnType,
  };
}

function param(index: number): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_PARAMETER,
    index,
    returnType: "ANY",
  };
}

const noBindings = (): ColumnBinding[] => [];

function makeScan(tableName: string): LogicalGet {
  return {
    type: LogicalOperatorType.LOGICAL_GET,
    tableName,
    tableIndex: 0,
    schema: { name: tableName, columns: [] },
    columnIds: [],
    tableFilters: [],
    children: [],
    expressions: [],
    types: [],
    estimatedCardinality: 0,
    getColumnBindings: noBindings,
  };
}

// ---------------------------------------------------------------------------
// formatExpression
// ---------------------------------------------------------------------------

describe("formatExpression", () => {
  it("formats column ref", () => {
    expect(formatExpression(colRef("users", "name"))).toBe("users.name");
  });

  it("formats string constant", () => {
    expect(formatExpression(constant("hello"))).toBe("'hello'");
  });

  it("formats numeric constant", () => {
    expect(formatExpression(constant(42))).toBe("42");
  });

  it("formats boolean constant", () => {
    expect(formatExpression(constant(true))).toBe("true");
  });

  it("formats null constant", () => {
    expect(formatExpression(constant(null))).toBe("NULL");
  });

  it("formats parameter", () => {
    expect(formatExpression(param(0))).toBe("$1");
    expect(formatExpression(param(2))).toBe("$3");
  });

  it("formats comparison", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: colRef("t", "age"),
      right: constant(18),
      returnType: "BOOLEAN",
    };
    expect(formatExpression(expr)).toBe("t.age > 18");
  });

  it("formats conjunction", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
      conjunctionType: "AND",
      children: [
        {
          expressionClass: BoundExpressionClass.BOUND_COMPARISON,
          comparisonType: "GREATER",
          left: colRef("t", "a"),
          right: constant(1),
          returnType: "BOOLEAN",
        },
        {
          expressionClass: BoundExpressionClass.BOUND_COMPARISON,
          comparisonType: "LESS",
          left: colRef("t", "b"),
          right: constant(10),
          returnType: "BOOLEAN",
        },
      ],
      returnType: "BOOLEAN",
    };
    expect(formatExpression(expr)).toBe("t.a > 1 AND t.b < 10");
  });

  it("formats arithmetic operator", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "ADD",
      children: [colRef("t", "x"), constant(1)],
      returnType: "INTEGER",
    };
    expect(formatExpression(expr)).toBe("t.x + 1");
  });

  it("formats IS NULL", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "IS_NULL",
      children: [colRef("t", "x")],
      returnType: "BOOLEAN",
    };
    expect(formatExpression(expr)).toBe("t.x IS NULL");
  });

  it("formats function call", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_FUNCTION,
      functionName: "UPPER",
      children: [colRef("t", "name")],
      returnType: "TEXT",
    };
    expect(formatExpression(expr)).toBe("UPPER(t.name)");
  });

  it("formats aggregate COUNT(*)", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
      functionName: "COUNT",
      isStar: true,
      distinct: false,
      children: [],
      aggregateIndex: 0,
      returnType: "INTEGER",
    };
    expect(formatExpression(expr)).toBe("COUNT(*)");
  });

  it("formats aggregate with DISTINCT", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
      functionName: "COUNT",
      isStar: false,
      distinct: true,
      children: [colRef("t", "val")],
      aggregateIndex: 0,
      returnType: "INTEGER",
    };
    expect(formatExpression(expr)).toBe("COUNT(DISTINCT t.val)");
  });

  it("formats BETWEEN", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_BETWEEN,
      input: colRef("t", "x"),
      lower: constant(1),
      upper: constant(10),
      returnType: "BOOLEAN",
    };
    expect(formatExpression(expr)).toBe("t.x BETWEEN 1 AND 10");
  });

  it("formats CAST", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_CAST,
      child: colRef("t", "x"),
      castType: "TEXT",
      returnType: "TEXT",
    };
    expect(formatExpression(expr)).toBe("CAST(t.x AS TEXT)");
  });

  it("formats JSON access", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        tableName: "t",
        columnName: "data",
        binding: { tableIndex: 0, columnIndex: 0 },
        returnType: "JSON",
      },
      path: [
        { type: "field", name: "items" },
        { type: "index", value: 0 },
        { type: "field", name: "title" },
      ],
      returnType: "JSON",
    };
    expect(formatExpression(expr)).toBe("t.data.items[0].title");
  });
});

// ---------------------------------------------------------------------------
// formatPlan
// ---------------------------------------------------------------------------

describe("formatPlan", () => {
  it("formats a single scan node", () => {
    const plan = formatPlan(makeScan("users"));
    expect(plan).toBe("Scan users");
  });

  it("formats scan with estimated cardinality", () => {
    const scan = makeScan("users");
    scan.estimatedCardinality = 100;
    expect(formatPlan(scan)).toBe("Scan users (~100 rows)");
  });

  it("formats index scan", () => {
    const scan = makeScan("users");
    scan.indexHint = {
      indexDef: { name: "idx_email", tableName: "users", expressions: [{ type: 'column', name: 'email', returnType: 'TEXT' }], unique: true },
      predicates: [],
      residualFilters: [],
      coveredFilters: [],
    };
    expect(formatPlan(scan)).toBe("IndexScan users (idx_email)");
  });

  it("formats nested plan with indentation", () => {
    const filter: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [makeScan("t")],
      expressions: [
        {
          expressionClass: BoundExpressionClass.BOUND_COMPARISON,
          comparisonType: "EQUAL",
          left: colRef("t", "id"),
          right: constant(1),
          returnType: "BOOLEAN",
        },
      ],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: noBindings,
    };

    const projection: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 0,
      children: [filter],
      expressions: [colRef("t", "id"), colRef("t", "name")],
      aliases: ["id", "name"],
      types: ["INTEGER", "TEXT"],
      estimatedCardinality: 0,
      getColumnBindings: noBindings,
    };

    const plan = formatPlan(projection);
    const lines = plan.split("\n");
    expect(lines).toHaveLength(3);
    expect(lines[0]).toBe("Projection (id, name)");
    expect(lines[1]).toBe("  Filter (t.id = 1)");
    expect(lines[2]).toBe("    Scan t");
  });

  it("formats join with two children", () => {
    const join: LogicalComparisonJoin = {
      type: LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
      joinType: "LEFT",
      children: [makeScan("a"), makeScan("b")],
      conditions: [
        {
          left: colRef("a", "id"),
          right: colRef("b", "a_id"),
          comparisonType: "EQUAL",
        },
      ],
      expressions: [],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: noBindings,
    };

    const plan = formatPlan(join);
    const lines = plan.split("\n");
    expect(lines[0]).toBe("HashJoin [LEFT] (a.id = b.a_id)");
    expect(lines[1]).toBe("  Scan a");
    expect(lines[2]).toBe("  Scan b");
  });
});
