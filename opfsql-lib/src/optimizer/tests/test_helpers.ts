import { expect } from "vitest";
import { Binder } from "../../binder/index.js";
import type {
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  BoundConstantExpression,
  BoundExpression,
  LogicalGet,
  LogicalOperator,
} from "../../binder/types.js";
import {
  BoundExpressionClass,
  LogicalOperatorType,
} from "../../binder/types.js";
import { Parser } from "../../parser/index.js";
import { Catalog } from "../../store/catalog.js";
import type { TableSchema } from "../../store/types.js";
import { pushdownFilters } from "../index.js";

// ============================================================================
// Parser
// ============================================================================

export const parser = new Parser();

export function parse(sql: string) {
  const stmts = parser.parse(sql);
  expect(stmts).toHaveLength(1);
  return stmts[0];
}

// ============================================================================
// Schemas
// ============================================================================

export const usersSchema: TableSchema = {
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
    {
      name: "active",
      type: "BOOLEAN",
      nullable: true,
      primaryKey: false,
      unique: false,
      autoIncrement: false,
      defaultValue: null,
    },
  ],
};

export const ordersSchema: TableSchema = {
  name: "orders",
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
      name: "user_id",
      type: "INTEGER",
      nullable: false,
      primaryKey: false,
      unique: false,
      autoIncrement: false,
      defaultValue: null,
    },
    {
      name: "amount",
      type: "REAL",
      nullable: true,
      primaryKey: false,
      unique: false,
      autoIncrement: false,
      defaultValue: null,
    },
    {
      name: "status",
      type: "TEXT",
      nullable: true,
      primaryKey: false,
      unique: false,
      autoIncrement: false,
      defaultValue: null,
    },
  ],
};

export const productsSchema: TableSchema = {
  name: "products",
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
      name: "price",
      type: "REAL",
      nullable: true,
      primaryKey: false,
      unique: false,
      autoIncrement: false,
      defaultValue: null,
    },
  ],
};

// ============================================================================
// Test context
// ============================================================================

export function createTestContext() {
  const catalog = new Catalog();
  catalog.addTable(usersSchema);
  catalog.addTable(ordersSchema);
  catalog.addTable(productsSchema);
  const binder = new Binder(catalog);
  const bind = (sql: string): LogicalOperator =>
    binder.bindStatement(parse(sql));
  return { catalog, binder, bind };
}

// ============================================================================
// Tree navigation helpers
// ============================================================================

export function findNode(
  plan: LogicalOperator,
  type: LogicalOperatorType,
): LogicalOperator | null {
  if (plan.type === type) return plan;
  for (const child of plan.children) {
    const found = findNode(child, type);
    if (found) return found;
  }
  return null;
}

export function findAllNodes(
  plan: LogicalOperator,
  type: LogicalOperatorType,
): LogicalOperator[] {
  const result: LogicalOperator[] = [];
  if (plan.type === type) result.push(plan);
  for (const child of plan.children) {
    result.push(...findAllNodes(child, type));
  }
  return result;
}

export function getGet(plan: LogicalOperator): LogicalGet {
  const node = findNode(plan, LogicalOperatorType.LOGICAL_GET);
  expect(node).not.toBeNull();
  return node as LogicalGet;
}

export function getAllGets(plan: LogicalOperator): LogicalGet[] {
  return findAllNodes(plan, LogicalOperatorType.LOGICAL_GET) as LogicalGet[];
}

// ============================================================================
// Expression construction helpers
// ============================================================================

export function makeColRef(
  tableIndex: number,
  columnIndex: number,
): BoundColumnRefExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
    binding: { tableIndex, columnIndex },
    tableName: "",
    columnName: "",
    returnType: "INTEGER",
  };
}

export function makeIntConstant(value: number): BoundConstantExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value,
    returnType: "INTEGER",
  };
}

export function makeStrConstant(value: string): BoundConstantExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value,
    returnType: "TEXT",
  };
}

export function containsFunction(expr: BoundExpression): boolean {
  if (expr.expressionClass === BoundExpressionClass.BOUND_FUNCTION) return true;
  if (expr.expressionClass === BoundExpressionClass.BOUND_COMPARISON) {
    const cmp = expr as BoundComparisonExpression;
    return containsFunction(cmp.left) || containsFunction(cmp.right);
  }
  if (expr.expressionClass === BoundExpressionClass.BOUND_CONJUNCTION) {
    return (expr as BoundConjunctionExpression).children.some(containsFunction);
  }
  return false;
}

// Helper: run pushdown for multi-way join tests
export function pushed(plan: LogicalOperator): LogicalOperator {
  return pushdownFilters(plan);
}
