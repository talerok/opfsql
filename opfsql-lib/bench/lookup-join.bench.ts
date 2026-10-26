// "An order with its user" in the executor, on a pager over memory files,
// over the shop of bench/scenarios.ts (1 000 users, 20 000 orders):
//   SELECT o.id, o.total, u.name FROM orders o JOIN users u ON u.id = o.user_id
//   WHERE o.id = $1
// as a lookup join, which seeks the user by the order's rowid, and as the
// hash join the optimizer chooses today, which hashes every user. Plans are
// built by hand until the optimizer chooses lookup joins. One query per
// iteration, so the mean is the time per query.
// Run with `npx vitest bench --run bench/lookup-join`.
import { bench, describe } from "vitest";
import { column } from "../src/expressions/tests/builders.js";
import type { physical } from "../src/sql/plan/index.js";
import { column as columnOf } from "../src/table/tests/database.js";
import { TestDatabase } from "../src/executor/tests/database.js";
import {
  hashJoin,
  lookupJoin,
  parameter,
  project,
  rowidList,
  source,
  tableScan,
} from "../src/executor/tests/plans.js";

const USER_COUNT = 1000;
const ORDER_COUNT = 20_000;
const ORDERS = 1;
const USERS = 2;
const SELECTED = 3;

/** Long enough runs, after a few untimed ones, for medians that repeat. */
const TIMING = { time: 3000, warmupIterations: 100 };

/** The order ids the queries ask for, scattered as the scenario does. */
function scatter(index: number, count: number): number {
  return (index * 7919) % count;
}

const database = await TestDatabase.open();
const users = database.createTable({
  name: "users",
  columns: [
    columnOf("id", "INTEGER"),
    columnOf("name", "TEXT"),
    columnOf("email", "TEXT"),
  ],
  rowidColumn: 0,
  isAutoincrement: false,
});
const orders = database.createTable({
  name: "orders",
  columns: [
    columnOf("id", "INTEGER"),
    columnOf("user_id", "INTEGER"),
    columnOf("total", "REAL"),
    columnOf("created_at", "INTEGER"),
  ],
  rowidColumn: 0,
  isAutoincrement: false,
});
database.insert(
  users,
  Array.from({ length: USER_COUNT }, (_, id) => [
    id,
    `User ${String(id)}`,
    `user${String(id)}@example.com`,
  ]),
);
database.insert(
  orders,
  Array.from({ length: ORDER_COUNT }, (_, id) => [
    id,
    scatter(id, USER_COUNT),
    ((id * 7919) % 100_000) / 100,
    id,
  ]),
);

const orderId = column(ORDERS, 0, "INTEGER");
const orderUser = column(ORDERS, 1, "INTEGER");
const orderTotal = column(ORDERS, 2, "REAL");
const userId = column(USERS, 0, "INTEGER");
const nameOfUser = column(USERS, 1, "TEXT");

/** The order $1, by its rowid. */
const theOrder = rowidList(source(orders, ORDERS, [0, 1, 2]), [parameter(0)]);

function selected(join: physical.Plan): physical.Plan {
  return project(SELECTED, join, orderId, orderTotal, nameOfUser);
}

const looked = selected(
  lookupJoin(theOrder, rowidList(source(users, USERS, [1]), [orderUser])),
);
const hashed = selected(
  hashJoin(
    [theOrder, tableScan(source(users, USERS, [0, 1]))],
    [[orderUser], [userId]],
  ),
);

let query = 0;
function nextOrder(): number {
  query++;
  return scatter(query, ORDER_COUNT);
}

describe("an order with its user", () => {
  bench(
    "lookup join, the user by rowid",
    () => {
      database.query(looked, [nextOrder()]);
    },
    TIMING,
  );
  bench(
    "hash join, every user hashed",
    () => {
      database.query(hashed, [nextOrder()]);
    },
    TIMING,
  );
});
