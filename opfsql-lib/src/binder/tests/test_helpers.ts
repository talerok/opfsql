import { expect } from "vitest";
import { Parser } from "../../parser/index.js";
import { Catalog } from "../../store/catalog.js";
import type { TableSchema } from "../../store/types.js";
import { Binder } from "../index.js";
import type { LogicalOperator } from "../types.js";

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

// ============================================================================
// Test context
// ============================================================================

export function createTestContext() {
  const catalog = new Catalog();
  catalog.addTable(usersSchema);
  catalog.addTable(ordersSchema);
  const binder = new Binder(catalog);
  const bind = (sql: string): LogicalOperator =>
    binder.bindStatement(parse(sql));
  return { catalog, binder, bind };
}
