import { beforeEach, describe, expect, it } from "vitest";
import { ConstraintError } from "../../errors.js";
import type {
  Index,
  IndexShape,
  Table,
  TableShape,
} from "../../schema/index.js";
import { constraintIndexName } from "../../schema/index.js";
import type {
  ConflictAction,
  OnConflict,
  physical,
} from "../../sql/plan/index.js";
import { ROWID_COLUMN } from "../../sql/plan/index.js";
import { column as columnOf } from "../../table/tests/database.js";
import type { Value } from "../../values/index.js";
import { TestDatabase } from "./database.js";
import {
  aggregateCall,
  arithmetic,
  between,
  column,
  compare,
  constant,
  deleteFrom,
  filter,
  hashAggregate,
  including,
  indexScan,
  insert,
  keys,
  NULL,
  outer,
  scalarSubquery,
  type Source,
  source,
  tableScan,
  update,
  values,
} from "./plans.js";

// INSERT, UPDATE and DELETE through the table layer, which keeps types,
// NOT NULL, rowids, unique keys, index entries and AUTOINCREMENT together:
//   items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, qty INTEGER DEFAULT 1,
//          code TEXT UNIQUE), indexed on qty.

const ITEMS = 3;
const qty = column(ITEMS, 2, "INTEGER");
const name = column(ITEMS, 1, "TEXT");
const code = column(ITEMS, 3, "TEXT");
const itemId = column(ITEMS, 0, "INTEGER");
const EVERY_COLUMN = [0, 1, 2, 3];

const itemsShape: TableShape = {
  name: "items",
  columns: [
    columnOf("id", "INTEGER"),
    columnOf("name", "TEXT", { isNotNull: true }),
    columnOf("qty", "INTEGER", { defaultValue: 1 }),
    columnOf("code", "TEXT"),
  ],
  rowidColumn: 0,
  isAutoincrement: false,
};

const uniqueCode: IndexShape = {
  name: constraintIndexName("items", 1),
  table: "items",
  keys: [{ kind: "column", column: 3 }],
  isUnique: true,
  origin: "unique",
};

let database: TestDatabase;
let items: Table;
let byCode: Index;
let byQty: Index;

beforeEach(async () => {
  database = await TestDatabase.open();
  items = database.createTable(itemsShape, [uniqueCode]);
  byCode = database.constraintIndex(items, 1);
  byQty = database.createIndex(
    {
      name: "items_qty",
      table: "items",
      keys: [{ kind: "column", column: 2 }],
      isUnique: false,
      origin: "statement",
    },
    items,
  );
  database.insert(items, [
    [1, "a", 1, "A"],
    [2, "b", 1, "B"],
  ]);
});

/** Items as an UPDATE reads them: every column, then the rowid. */
function updated(): Source {
  return source(items, ITEMS, [...EVERY_COLUMN, ROWID_COLUMN]);
}

/** One column of every item, in rowid order. */
function columnOfItems(position: number): Value[] {
  return database.rows(items).map((row) => row[position] ?? null);
}

/** VALUES of literal rows, as table 70. */
function rows(...literal: Value[][]): physical.Values {
  return values(
    70,
    ...literal.map((row) => row.map((value) => constant(value))),
  );
}

/** INSERT INTO items VALUES … ON CONFLICT (target) action. */
function upsert(
  literal: Value[][],
  target: OnConflict<physical.Plan>["target"],
  action: ConflictAction<physical.Plan>,
): ReturnType<typeof insert> {
  return insert(items, EVERY_COLUMN, rows(...literal), { target, action });
}

/** DO UPDATE SET qty = items.qty + excluded.qty [WHERE excluded.qty > 0]. */
function addQuantity(isConditional: boolean): ConflictAction<physical.Plan> {
  const existing = column(80, 2, "INTEGER");
  const refused = column(81, 2, "INTEGER");
  return {
    kind: "update",
    targetIndex: 80,
    excludedIndex: 81,
    assignments: [{ column: 2, value: arithmetic(existing, "+", refused) }],
    condition: isConditional ? compare(refused, ">", constant(0)) : null,
  };
}

describe("INSERT", () => {
  it("fills omitted columns with their defaults, and reports changes and the last rowid", () => {
    const outcome = database.run(
      insert(items, [1, 3], rows(["c", "C"], ["d", null])),
    );
    expect(outcome).toEqual({ changes: 2, lastInsertRowid: 4 });
    expect(database.rows(items).slice(2)).toEqual([
      [3, "c", 1, "C"],
      [4, "d", 1, null],
    ]);
  });

  it("refuses a row that breaks NOT NULL or UNIQUE, as the table layer decides", () => {
    const nameless = insert(items, [1], values(70, [NULL]));
    expect(() => database.run(nameless)).toThrow(
      new ConstraintError("not-null", "items", ["name"]),
    );
    const taken = insert(items, [1, 3], rows(["c", "A"]));
    expect(() => database.run(taken)).toThrow(
      "UNIQUE constraint failed: items.code",
    );
    expect(database.rows(items)).toHaveLength(2);
  });

  it("reads all of its source before inserting into the table it reads", () => {
    const copies = tableScan(source(items, ITEMS, [1, 2]));
    const outcome = database.run(insert(items, [1, 2], copies));
    expect(outcome.changes).toBe(2);
    expect(database.rows(items).map(([id]) => id)).toEqual([1, 2, 3, 4]);
  });

  it("never issues an AUTOINCREMENT rowid twice", async () => {
    const fresh = await TestDatabase.open();
    const tickets = fresh.createTable({
      name: "tickets",
      columns: [columnOf("id", "INTEGER"), columnOf("title", "TEXT")],
      rowidColumn: 0,
      isAutoincrement: true,
    });
    const add = insert(tickets, [1], values(70, [constant("t")]));
    fresh.run(add);
    fresh.run(add);
    const all = source(tickets, 4, [ROWID_COLUMN]);
    expect(fresh.run(deleteFrom(all, tableScan(all))).changes).toBe(2);
    expect(fresh.run(add).lastInsertRowid).toBe(3);
  });
});

describe("ON CONFLICT", () => {
  it("DO NOTHING skips a row in the way under its constraint", () => {
    const action = { kind: "nothing" } as const;
    const outcome = database.run(
      upsert(
        [
          [null, "x", 1, "A"],
          [null, "y", 1, "C"],
        ],
        byCode,
        action,
      ),
    );
    expect(outcome).toEqual({ changes: 1, lastInsertRowid: 3 });
    expect(database.rows(items).map(([, itemName]) => itemName)).toEqual([
      "a",
      "b",
      "y",
    ]);
  });

  it("still fails for a row in the way under another constraint", () => {
    const action = { kind: "nothing" } as const;
    const statement = upsert([[null, "x", 1, "A"]], "rowid", action);
    expect(() => database.run(statement)).toThrow(ConstraintError);
    const anyConstraint = upsert([[1, "x", 1, "Z"]], null, action);
    expect(database.run(anyConstraint).changes).toBe(0);
  });

  it("DO UPDATE sets the row in the way from it and the refused row", () => {
    const statement = upsert([[1, "x", "5", "Z"]], "rowid", addQuantity(false));
    expect(database.run(statement)).toEqual({
      changes: 1,
      lastInsertRowid: null,
    });
    expect(database.rows(items)[0]).toEqual([1, "a", 6, "A"]);
  });

  it("DO UPDATE … WHERE changes nothing when its condition is not TRUE", () => {
    const statement = upsert([[null, "x", -5, "A"]], byCode, addQuantity(true));
    expect(database.run(statement).changes).toBe(0);
    expect(database.rows(items)[0]).toEqual([1, "a", 1, "A"]);
  });
});

describe("UPDATE", () => {
  it("reads every row first, so moving rows within the index it scans changes each once", () => {
    const scan = indexScan(
      updated(),
      byQty,
      keys([], between(including(constant(1)), null)),
    );
    const outcome = database.run(
      update(updated(), scan, [
        { column: 2, value: arithmetic(qty, "+", constant(10)) },
      ]),
    );
    expect(outcome).toEqual({ changes: 2, lastInsertRowid: null });
    expect(columnOfItems(2)).toEqual([11, 11]);
    expect(database.problems()).toEqual([]);
  });

  it("computes every assignment from the old row", () => {
    const swap = [
      { column: 1, value: code },
      { column: 3, value: name },
    ];
    database.run(update(updated(), tableScan(updated()), swap));
    expect(database.rows(items)).toEqual([
      [1, "A", 1, "a"],
      [2, "B", 1, "b"],
    ]);
  });

  it("moves a row whose rowid column it sets, and refuses a broken constraint", () => {
    const moved = [{ column: 0, value: arithmetic(itemId, "+", constant(10)) }];
    database.run(update(updated(), tableScan(updated()), moved));
    expect(database.rows(items).map(([id]) => id)).toEqual([11, 12]);
    const clash = [{ column: 3, value: constant("same") }];
    expect(() =>
      database.run(update(updated(), tableScan(updated()), clash)),
    ).toThrow("UNIQUE constraint failed: items.code");
    expect(columnOfItems(3)).toEqual(["A", "B"]);
    expect(database.problems()).toEqual([]);
  });
});

/** The table indexes of t, updated, and of u, the same table read in the subquery. */
const T_INDEX = 3;
const U_INDEX = 4;

describe("UPDATE from the table as it was", () => {
  it("computes every row's value before writing any: running sums 1, 3, 6", async () => {
    // UPDATE t SET x = (SELECT sum(x) FROM t AS u WHERE u.id <= t.id)
    const fresh = await TestDatabase.open();
    const t = fresh.createTable({
      name: "t",
      columns: [columnOf("id", "INTEGER"), columnOf("x", "INTEGER")],
      rowidColumn: 0,
      isAutoincrement: false,
    });
    fresh.insert(t, [
      [1, 1],
      [2, 2],
      [3, 3],
    ]);
    const target = source(t, T_INDEX, [0, 1, ROWID_COLUMN]);
    const upToHere = filter(
      tableScan(source(t, U_INDEX, [0, 1])),
      compare(column(U_INDEX, 0, "INTEGER"), "<=", outer(1, T_INDEX, 0)),
    );
    const sum = hashAggregate(
      [20, 21],
      upToHere,
      [],
      [aggregateCall("sum", [column(U_INDEX, 1, "INTEGER")])],
    );
    const running = [{ column: 1, value: scalarSubquery(sum, "correlated") }];
    fresh.run(update(target, tableScan(target), running));
    expect(fresh.rows(t)).toEqual([
      [1, 1],
      [2, 3],
      [3, 6],
    ]);
  });
});

describe("DELETE", () => {
  it("reads the rowids first, then deletes each row and its index entries", () => {
    database.insert(items, [[3, "c", 2, null]]);
    const doomed = source(items, ITEMS, [ROWID_COLUMN]);
    const scan = indexScan(doomed, byQty, keys([constant(1)]));
    expect(database.run(deleteFrom(doomed, scan))).toEqual({
      changes: 2,
      lastInsertRowid: null,
    });
    expect(database.rows(items)).toEqual([[3, "c", 2, null]]);
    expect(database.problems()).toEqual([]);
  });

  it("finds the rowid by its binding, whatever columns its source has", () => {
    database.insert(items, [[3, "c", 2, null]]);
    const narrow = source(items, ITEMS, [ROWID_COLUMN]);
    const atTwo = indexScan(narrow, byQty, keys([constant(2)]));
    expect(database.run(deleteFrom(narrow, atTwo)).changes).toBe(1);
    const wide = source(items, ITEMS, [...EVERY_COLUMN, ROWID_COLUMN]);
    const named = filter(tableScan(wide), compare(name, "=", constant("a")));
    expect(database.run(deleteFrom(wide, named)).changes).toBe(1);
    expect(database.rows(items)).toEqual([[2, "b", 1, "B"]]);
  });
});
