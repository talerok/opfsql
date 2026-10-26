import { column as reference } from "../../expressions/tests/builders.js";
import type {
  Index,
  IndexKey,
  IndexShape,
  Table,
  TableShape,
} from "../../schema/index.js";
import { constraintIndexName } from "../../schema/index.js";
import { column } from "../../table/tests/database.js";
import { TestDatabase } from "./database.js";

// The two tables most executor tests read:
//
//   users                                orders
//   id │ name │ age  │ email             id │ user │ amount │ note (ANY)
//   ───┼──────┼──────┼──────             ───┼──────┼────────┼──────────
//    1 │ Ann  │ 30   │ a@x               10 │ 1    │ 9.5    │ 'x'
//    2 │ bob  │ 25   │ b@x               11 │ 1    │ 20     │ NULL
//    3 │ Cid  │ NULL │ c@x               12 │ 2    │ 7      │ '1' (text)
//    4 │ dan  │ 30   │ NULL              13 │ NULL │ 1      │ NULL
//    5 │ Eve  │ 41   │ e@x               14 │ 9    │ 3      │ NULL
//
// users.id and orders.id are the rowids; users.email is UNIQUE; users is
// indexed on age, on (age, name) and on lower(name). User 9 does not exist.

/** The table index plans give users, and the references to its columns. */
export const USERS = 1;
export const id = reference(USERS, 0, "INTEGER");
export const name = reference(USERS, 1, "TEXT");
export const age = reference(USERS, 2, "INTEGER");
export const email = reference(USERS, 3, "TEXT");

/** The table index plans give orders, and the references to its columns. */
export const ORDERS = 2;
export const orderId = reference(ORDERS, 0, "INTEGER");
export const orderUser = reference(ORDERS, 1, "INTEGER");
export const amount = reference(ORDERS, 2, "REAL");
export const note = reference(ORDERS, 3, "ANY");

/** The table index of users read a second time, as in a self-join. */
export const OTHER_USERS = 3;
/** The table index of a query's select list. */
export const SELECTED = 9;

export interface Fixture {
  readonly database: TestDatabase;
  readonly users: Table;
  readonly orders: Table;
  readonly byAge: Index;
  readonly byAgeName: Index;
  readonly byLowerName: Index;
}

const usersShape: TableShape = {
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

export async function fixture(): Promise<Fixture> {
  const database = await TestDatabase.open();
  const users = database.createTable(usersShape, [uniqueEmail]);
  const orders = database.createTable(ordersShape);
  database.insert(users, [
    [1, "Ann", 30, "a@x"],
    [2, "bob", 25, "b@x"],
    [3, "Cid", null, "c@x"],
    [4, "dan", 30, null],
    [5, "Eve", 41, "e@x"],
  ]);
  database.insert(orders, [
    [10, 1, 9.5, "x"],
    [11, 1, 20, null],
    [12, 2, 7, "1"],
    [13, null, 1, null],
    [14, 9, 3, null],
  ]);
  const ageKey: IndexKey = { kind: "column", column: 2 };
  const nameKey: IndexKey = { kind: "column", column: 1 };
  const lowerName: IndexKey = {
    kind: "expression",
    sql: "lower(name)",
    type: "TEXT",
  };
  return {
    database,
    users,
    orders,
    byAge: database.createIndex(usersIndex("users_age", [ageKey]), users),
    byAgeName: database.createIndex(
      usersIndex("users_age_name", [ageKey, nameKey]),
      users,
    ),
    byLowerName: database.createIndex(
      usersIndex("users_lower_name", [lowerName]),
      users,
    ),
  };
}

function usersIndex(indexName: string, keys: IndexShape["keys"]): IndexShape {
  return {
    name: indexName,
    table: "users",
    keys,
    isUnique: false,
    origin: "statement",
  };
}
