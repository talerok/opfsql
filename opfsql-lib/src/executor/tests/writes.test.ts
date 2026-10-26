import { beforeEach, describe, expect, it } from "vitest";
import { ConstraintError } from "../../errors.js";
import type {
  Index,
  IndexKey,
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
import type { CompileExpression } from "../../table/index.js";
import { column as columnOf } from "../../table/tests/database.js";
import type { Value } from "../../values/index.js";
import { BATCH_SIZE } from "../operator.js";
import { TestDatabase } from "./database.js";
import {
  aggregateCall,
  arithmetic,
  between,
  column,
  compare,
  constant,
  Counting,
  deleteFrom,
  filter,
  hashAggregate,
  including,
  indexScan,
  insert,
  keys,
  NULL,
  outer,
  project,
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

/** Table indexes: items, a second table, literal rows, and DO UPDATE's two rows. */
const ITEMS = 3;
const TICKETS = 4;
const LITERALS = 70;
const IN_THE_WAY = 80;
const EXCLUDED = 81;
const SUMMED = 20;
/** The table indexes of t, updated, and of u, the same table read in its subquery. */
const T_INDEX = 3;
const U_INDEX = 4;

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

/** VALUES of literal rows. */
function rows(...literal: Value[][]): physical.Values {
  return values(
    LITERALS,
    ...literal.map((row) => row.map((value) => constant(value))),
  );
}

/** INSERT INTO items VALUES … ON CONFLICT (target) action. */
function upsert(
  literal: Value[][],
  target: OnConflict<physical.Plan>["target"],
  action: ConflictAction<physical.Plan>,
): ReturnType<typeof insert> {
  const onConflict = { target, action };
  return insert(items, EVERY_COLUMN, rows(...literal), { onConflict });
}

/** DO UPDATE SET qty = items.qty + excluded.qty [WHERE excluded.qty > 0]. */
function addQuantity(isConditional: boolean): ConflictAction<physical.Plan> {
  const existing = column(IN_THE_WAY, 2, "INTEGER");
  const refused = column(EXCLUDED, 2, "INTEGER");
  return {
    kind: "update",
    targetIndex: IN_THE_WAY,
    excludedIndex: EXCLUDED,
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
    const nameless = insert(items, [1], values(LITERALS, [NULL]));
    expect(() => database.run(nameless)).toThrow(
      new ConstraintError("not-null", "items", ["name"]),
    );
    const taken = insert(items, [1, 3], rows(["c", "A"]));
    expect(() => database.run(taken)).toThrow(
      "UNIQUE constraint failed: items.code",
    );
    expect(database.rows(items)).toEqual([
      [1, "a", 1, "A"],
      [2, "b", 1, "B"],
    ]);
  });

  it("reads all of its source before inserting into the table it reads", () => {
    const copies = tableScan(source(items, ITEMS, [1, 2]));
    const doubling = insert(items, [1, 2], copies, { readsTarget: true });
    const outcome = database.run(doubling);
    expect(outcome).toEqual({ changes: 2, lastInsertRowid: 4 });
    expect(database.rows(items)).toEqual([
      [1, "a", 1, "A"],
      [2, "b", 1, "B"],
      [3, "a", 1, null],
      [4, "b", 1, null],
    ]);
  });

  it("never issues an AUTOINCREMENT rowid twice", async () => {
    const fresh = await TestDatabase.open();
    const tickets = fresh.createTable({
      name: "tickets",
      columns: [columnOf("id", "INTEGER"), columnOf("title", "TEXT")],
      rowidColumn: 0,
      isAutoincrement: true,
    });
    const add = insert(tickets, [1], values(LITERALS, [constant("t")]));
    fresh.run(add);
    fresh.run(add);
    const all = source(tickets, TICKETS, [ROWID_COLUMN]);
    expect(fresh.run(deleteFrom(all, tableScan(all))).changes).toBe(2);
    expect(fresh.run(add).lastInsertRowid).toBe(3);
  });
});

/** Table indexes of a copy's source table and of its select list. */
const COPIED = 5;
const COPYING = 6;
const COPIED_ROWS = 600;

/** A table (id INTEGER PRIMARY KEY, label TEXT) to copy from or to. */
function labelledShape(tableName: string): TableShape {
  return {
    name: tableName,
    columns: [columnOf("id", "INTEGER"), columnOf("label", "TEXT")],
    rowidColumn: 0,
    isAutoincrement: false,
  };
}

/**
 * An index key on label that, computed for the first row written, notes how
 * many rows the source had computed by then.
 */
class FirstWrite {
  computed: number | null = null;

  readonly compile: CompileExpression = () => (row) => this.keyOf(row);

  constructor(private readonly counting: Counting) {}

  private keyOf(row: readonly Value[]): Value {
    this.computed ??= this.counting.calls;
    return row[1] ?? null;
  }
}

/**
 * INSERT INTO dst SELECT … FROM src, src holding 600 rows, its source read
 * as `reading` says: how many source rows had been computed when dst's first
 * row was written.
 */
async function computedAtFirstWrite(
  reading: "as it comes" | "whole first",
): Promise<number | null> {
  const counting = new Counting();
  const firstWrite = new FirstWrite(counting);
  const fresh = await TestDatabase.open(firstWrite.compile);
  const from = fresh.createTable(labelledShape("src"));
  const to = fresh.createTable(labelledShape("dst"));
  const label: IndexKey = { kind: "expression", sql: "label", type: "TEXT" };
  const byLabel = { name: "dst_label", table: "dst", keys: [label] } as const;
  fresh.createIndex({ ...byLabel, isUnique: false, origin: "statement" }, to);
  const labelled = Array.from({ length: COPIED_ROWS }, (_, index) => [
    index + 1,
    `row ${String(index + 1)}`,
  ]);
  fresh.insert(from, labelled);
  const copied = project(
    COPYING,
    tableScan(source(from, COPIED, [0, 1])),
    counting.of(column(COPIED, 0, "INTEGER")),
    column(COPIED, 1, "TEXT"),
  );
  fresh.run(
    insert(to, [0, 1], copied, { readsTarget: reading === "whole first" }),
  );
  expect(fresh.rows(to)).toEqual(labelled);
  return firstWrite.computed;
}

describe("INSERT … SELECT", () => {
  it("writes rows as its source produces them, when the source does not read the target", async () => {
    // One batch computed before the first write, not the whole source.
    expect(await computedAtFirstWrite("as it comes")).toBe(BATCH_SIZE);
  });

  it("reads its whole source first when the source reads the target", async () => {
    expect(await computedAtFirstWrite("whole first")).toBe(COPIED_ROWS);
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
    expect(database.rows(items)).toEqual([
      [1, "a", 1, "A"],
      [2, "b", 1, "B"],
      [3, "y", 1, "C"],
    ]);
  });

  it("still fails for a row in the way under another constraint", () => {
    const action = { kind: "nothing" } as const;
    const statement = upsert([[null, "x", 1, "A"]], "rowid", action);
    expect(() => database.run(statement)).toThrow(
      "UNIQUE constraint failed: items.code",
    );
    const anyConstraint = upsert([[1, "x", 1, "Z"]], null, action);
    expect(database.run(anyConstraint).changes).toBe(0);
  });

  it("finds its index by name, whichever load of the schema the plan's came from", () => {
    // In the way under the rowid and under code: the named index comes first.
    const reloaded = structuredClone(byCode);
    const statement = upsert([[1, "x", 1, "A"]], reloaded, { kind: "nothing" });
    expect(database.run(statement)).toEqual({
      changes: 0,
      lastInsertRowid: null,
    });
  });

  it("DO UPDATE sets the row in the way from it and the refused row", () => {
    const statement = upsert([[1, "x", "5", "Z"]], "rowid", addQuantity(false));
    expect(database.run(statement)).toEqual({
      changes: 1,
      lastInsertRowid: null,
    });
    expect(database.rows(items)).toEqual([
      [1, "a", 6, "A"],
      [2, "b", 1, "B"],
    ]);
  });

  it("DO UPDATE … WHERE changes nothing when its condition is not TRUE", () => {
    const statement = upsert([[null, "x", -5, "A"]], byCode, addQuantity(true));
    expect(database.run(statement).changes).toBe(0);
    expect(database.rows(items)).toEqual([
      [1, "a", 1, "A"],
      [2, "b", 1, "B"],
    ]);
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
    expect(database.rows(items)).toEqual([
      [11, "a", 1, "A"],
      [12, "b", 1, "B"],
    ]);
    const clash = [{ column: 3, value: constant("same") }];
    expect(() =>
      database.run(update(updated(), tableScan(updated()), clash)),
    ).toThrow("UNIQUE constraint failed: items.code");
    expect(columnOfItems(3)).toEqual(["A", "B"]);
    expect(database.problems()).toEqual([]);
  });
});

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
      [SUMMED, SUMMED + 1],
      upToHere,
      [],
      [aggregateCall("sum", [column(U_INDEX, 1, "INTEGER")])],
    );
    const running = [{ column: 1, value: scalarSubquery(sum, "correlated") }];
    fresh.run(
      update(target, tableScan(target), running, { readsTarget: true }),
    );
    expect(fresh.rows(t)).toEqual([
      [1, 1],
      [2, 3],
      [3, 6],
    ]);
  });
});

/** The table indexes of a table of 10 000 rows, and of its x index. */
const COUNTED = 7;
const COUNTED_ROWS = 10_000;

/** The counts an UPDATE had reached when it wrote its first row. */
interface AtFirstWrite {
  /** Rows its source had produced: rowids found. */
  readonly found: number;
  /** New rows it had computed, each from a row read by its rowid. */
  readonly computed: number;
}

/**
 * An index key on x that, once armed, notes the counts at the first row
 * written: the first key computed, for the old row the editor replaces.
 */
class FirstUpdate {
  isArmed = false;
  noted: AtFirstWrite | null = null;
  readonly found = new Counting();
  readonly computed = new Counting();
  readonly compile: CompileExpression = () => (row) => this.keyOf(row);

  private keyOf(row: readonly Value[]): Value {
    if (this.isArmed) {
      const { found, computed } = this;
      this.noted ??= { found: found.calls, computed: computed.calls };
    }
    return row[1] ?? null;
  }
}

/**
 * UPDATE t SET x = x + 1 over 10 000 rows, found by a scan that reads only
 * their rowids, as the optimizer narrows it: x is read with each row, by
 * its rowid. What the update had counted at its first write, and after.
 */
async function updateCounted(
  isTargetRead: boolean,
): Promise<{ atFirstWrite: AtFirstWrite | null; after: AtFirstWrite }> {
  const counts = new FirstUpdate();
  const fresh = await TestDatabase.open(counts.compile);
  const t = fresh.createTable({
    name: "t",
    columns: [columnOf("id", "INTEGER"), columnOf("x", "INTEGER")],
    rowidColumn: 0,
    isAutoincrement: false,
  });
  const x: IndexKey = { kind: "expression", sql: "x", type: "INTEGER" };
  const byX = { name: "t_x", table: "t", keys: [x] } as const;
  fresh.createIndex({ ...byX, isUnique: false, origin: "statement" }, t);
  fresh.insert(
    t,
    Array.from({ length: COUNTED_ROWS }, (_, index) => [index + 1, index]),
  );

  const rowids = source(t, COUNTED, [ROWID_COLUMN]);
  const rowid = column(COUNTED, ROWID_COLUMN, "INTEGER");
  const found = compare(counts.found.of(rowid), ">", constant(0));
  const plusOne = arithmetic(column(COUNTED, 1, "INTEGER"), "+", constant(1));
  const assignments = [{ column: 1, value: counts.computed.of(plusOne) }];
  counts.isArmed = true;
  const outcome = fresh.run(
    update(rowids, filter(tableScan(rowids), found), assignments, {
      readsTarget: isTargetRead,
    }),
  );
  expect(outcome.changes).toBe(COUNTED_ROWS);
  const xs = fresh.rows(t).map((row) => row[1]);
  expect(xs).toEqual(Array.from({ length: COUNTED_ROWS }, (_, n) => n + 1));
  const after = { found: counts.found.calls, computed: counts.computed.calls };
  return { atFirstWrite: counts.noted, after };
}

describe("UPDATE, one row at a time", () => {
  it("holds rowids, not rows: each row is read by its rowid and written before the next is read", async () => {
    const all = { found: COUNTED_ROWS, computed: COUNTED_ROWS };
    expect(await updateCounted(false)).toEqual({
      atFirstWrite: { found: COUNTED_ROWS, computed: 1 },
      after: all,
    });
  });

  it("computes every new row before writing any when an assignment reads the target", async () => {
    const all = { found: COUNTED_ROWS, computed: COUNTED_ROWS };
    expect(await updateCounted(true)).toEqual({
      atFirstWrite: all,
      after: all,
    });
  });
});

describe("DELETE", () => {
  it("deletes the rows an index scan finds, their index entries with them", () => {
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
