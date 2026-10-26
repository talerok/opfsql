// Joins in the executor, on a pager over memory files: a hash join of
// 50k × 50k rows and a nested-loop join of 1k × 1k, each taken whole and
// under LIMIT 10. Run with `npx vitest bench --run bench/executor-joins`.
import { bench, describe } from "vitest";
import { column } from "../src/expressions/tests/builders.js";
import type { physical } from "../src/sql/plan/index.js";
import type { Table } from "../src/schema/index.js";
import { column as columnOf } from "../src/table/tests/database.js";
import { TestDatabase } from "../src/executor/tests/database.js";
import {
  compare,
  constant,
  hashJoin,
  limit,
  nestedLoopJoin,
  source,
  tableScan,
} from "../src/executor/tests/plans.js";

const LEFT = 1;
const RIGHT = 2;
const database = await TestDatabase.open();

/** Long enough runs, after a few untimed ones, for medians that repeat. */
const TIMING = { time: 3000, warmupIterations: 3 };

/** A table (id INTEGER PRIMARY KEY, k INTEGER) of `count` rows, k running down from `count`. */
function keyedTable(name: string, count: number): Table {
  const table = database.createTable({
    name,
    columns: [columnOf("id", "INTEGER"), columnOf("k", "INTEGER")],
    rowidColumn: 0,
    isAutoincrement: false,
  });
  const rows = Array.from({ length: count }, (_, index) => [
    index + 1,
    count - index,
  ]);
  database.insert(table, rows);
  return table;
}

/** left JOIN right ON left.k = right.k, by `join`. */
function joinOf(
  left: Table,
  right: Table,
  join: "hash" | "nested-loop",
): physical.Plan {
  const leftScan = tableScan(source(left, LEFT, [0, 1]));
  const rightScan = tableScan(source(right, RIGHT, [0, 1]));
  const leftKey = column(LEFT, 1, "INTEGER");
  const rightKey = column(RIGHT, 1, "INTEGER");
  if (join === "hash") {
    return hashJoin([leftScan, rightScan], [[leftKey], [rightKey]]);
  }
  return nestedLoopJoin(leftScan, rightScan, [compare(leftKey, "=", rightKey)]);
}

const hashed = joinOf(keyedTable("a", 50_000), keyedTable("b", 50_000), "hash");
const looped = joinOf(
  keyedTable("c", 1000),
  keyedTable("d", 1000),
  "nested-loop",
);

describe("hash join, 50k × 50k", () => {
  bench(
    "every row",
    () => {
      database.query(hashed);
    },
    TIMING,
  );
  bench(
    "LIMIT 10",
    () => {
      database.query(limit(hashed, constant(10)));
    },
    TIMING,
  );
});

describe("nested-loop join, 1k × 1k", () => {
  bench(
    "every row",
    () => {
      database.query(looped);
    },
    TIMING,
  );
  bench(
    "LIMIT 10",
    () => {
      database.query(limit(looped, constant(10)));
    },
    TIMING,
  );
});
