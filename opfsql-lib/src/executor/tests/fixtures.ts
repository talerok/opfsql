import type {
  Index,
  IndexShape,
  Table,
  TableShape,
} from "../../schema/index.js";
import { constraintIndexName } from "../../schema/index.js";
import { ROWID_COLUMN } from "../../sql/plan/index.js";
import { column as reference } from "../../expressions/tests/builders.js";
import { column } from "../../table/tests/database.js";
import { TestDatabase } from "./database.js";

// The tables most executor tests read:
//   users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT UNIQUE)
//     with indexes on age, on (age, name) and on lower(name);
//   orders (id INTEGER PRIMARY KEY, user INTEGER, amount REAL, note ANY).

/** The table index plans give users, and their column references. */
export const USERS = 1;
export const id = reference(USERS, 0, "INTEGER");
export const name = reference(USERS, 1, "TEXT");
export const age = reference(USERS, 2, "INTEGER");
export const email = reference(USERS, 3, "TEXT");
export const userRowid = reference(USERS, ROWID_COLUMN, "INTEGER");
export const ALL_USER_COLUMNS = [0, 1, 2, 3];

export const ORDERS = 2;
export const orderId = reference(ORDERS, 0, "INTEGER");
export const orderUser = reference(ORDERS, 1, "INTEGER");
export const amount = reference(ORDERS, 2, "REAL");
export const note = reference(ORDERS, 3, "ANY");
export const ALL_ORDER_COLUMNS = [0, 1, 2, 3];

export const usersShape: TableShape = {
  name: "users",
  columns: [
    column("id", "INTEGER"),
    column("name", "TEXT"),
    column("age", "INTEGER"),
    column("email", "TEXT"),
  ],
  rowidColumn: 0,
  isAutoincrement: false,
};

const uniqueEmail: IndexShape = {
  name: constraintIndexName("users", 1),
  table: "users",
  keys: [{ kind: "column", column: 3 }],
  isUnique: true,
  origin: "unique",
};

const ordersShape: TableShape = {
  name: "orders",
  columns: [
    column("id", "INTEGER"),
    column("user", "INTEGER"),
    column("amount", "REAL"),
    column("note", "ANY"),
  ],
  rowidColumn: 0,
  isAutoincrement: false,
};

export const USER_ROWS = [
  [1, "Ann", 30, "a@x"],
  [2, "bob", 25, "b@x"],
  [3, "Cid", null, "c@x"],
  [4, "dan", 30, null],
  [5, "Eve", 41, "e@x"],
];

export const ORDER_ROWS = [
  [10, 1, 9.5, "x"],
  [11, 1, 20, null],
  [12, 2, 7, "1"],
  [13, null, 1, null],
  [14, 9, 3, null],
];

export interface Fixture {
  readonly database: TestDatabase;
  readonly users: Table;
  readonly orders: Table;
  readonly byEmail: Index;
  readonly byAge: Index;
  readonly byAgeName: Index;
  readonly byLowerName: Index;
}

export async function fixture(): Promise<Fixture> {
  const database = await TestDatabase.open();
  const users = database.createTable(usersShape, [uniqueEmail]);
  const orders = database.createTable(ordersShape);
  database.insert(users, USER_ROWS);
  database.insert(orders, ORDER_ROWS);
  return {
    database,
    users,
    orders,
    byEmail: database.constraintIndex(users, 1),
    byAge: database.createIndex(statementIndex("users_age", [2]), users),
    byAgeName: database.createIndex(
      statementIndex("users_age_name", [2, 1]),
      users,
    ),
    byLowerName: database.createIndex(
      {
        ...statementIndex("users_lower_name", []),
        keys: [{ kind: "expression", sql: "lower(name)", type: "TEXT" }],
      },
      users,
    ),
  };
}

function statementIndex(indexName: string, columns: number[]): IndexShape {
  return {
    name: indexName,
    table: "users",
    keys: columns.map((position) => ({ kind: "column", column: position })),
    isUnique: false,
    origin: "statement",
  };
}
