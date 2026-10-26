import { beforeEach, describe, expect, it } from "vitest";
import { CorruptError } from "../../errors.js";
import {
  type Index,
  type IndexShape,
  ROWID_COLUMN,
  type Table,
} from "../../schema/index.js";
import { encodeKey, IndexWriter, type ScanOrder } from "../../storage/index.js";
import { read, write } from "../../storage/tests/pager-support.js";
import type { Value } from "../../values/index.js";
import {
  IndexRows,
  type KeyBound,
  type KeyRange,
  type KeySeek,
  TableRows,
} from "../index.js";
import type { IndexEnd } from "../index-rows.js";
import { type Database, withUsers } from "./database.js";

// users (id INTEGER PRIMARY KEY, nick TEXT, email TEXT UNIQUE), indexed on
// nick and on (nick, email). The nick index holds, in key order:
// (NULL, 3), ('ann', 1), ('bob', 2), ('bob', 4), ('cid', 5).

const USER_ROWS: Value[][] = [
  [1, "ann", "a@x"],
  [2, "bob", null],
  [3, null, "c@x"],
  [4, "bob", "d@x"],
  [5, "cid", "e@x"],
];

let database: Database;
let users: Table;
let byNick: Index;
let byNickEmail: Index;

beforeEach(async () => {
  ({ database, users } = await withUsers());
  database.insert(users, USER_ROWS);
  byNick = createIndex("users_nick", [1]);
  byNickEmail = createIndex("users_nick_email", [1, 2]);
});

function createIndex(name: string, columns: number[]): Index {
  const shape: IndexShape = {
    name,
    tableName: "users",
    keys: columns.map((column) => ({ kind: "column", column })),
    isUnique: false,
    origin: "statement",
  };
  const index = database.change((catalog) => catalog.createIndex(shape, users));
  database.indexes.push(index);
  return index;
}

function including(value: Value): KeyBound {
  return { value, isInclusive: true };
}

function excluding(value: Value): KeyBound {
  return { value, isInclusive: false };
}

function within(lower: KeyBound | null, upper: KeyBound | null): KeyRange {
  return { lower, upper };
}

/** What `read` gives through `index`, in a read transaction. */
function through<T>(index: Index, use: (rows: IndexRows) => T): T {
  return read(database.pager, (pages) => use(new IndexRows(pages, index)));
}

function rowidsOf(
  index: Index,
  seek: KeySeek,
  order: ScanOrder = "ascending",
): number[] {
  return through(index, (rows) => [...rows.rowids(seek, order)]);
}

function nicksIn(range: KeyRange): number[] {
  return rowidsOf(byNick, { equal: [], range });
}

/** The rowids of bob's entries in the (nick, email) index, the email in `range`. */
function bobs(range: KeyRange | null): number[] {
  return rowidsOf(byNickEmail, { equal: ["bob"], range });
}

function extreme(index: Index, equal: Value[], end: IndexEnd): Value {
  return through(index, (rows) => rows.extreme(equal, end));
}

/** The ids of users from rowid `first` to `last`, read as a ColumnReader of id alone reads them. */
function between(
  first: number,
  last: number,
  order: ScanOrder = "ascending",
): Value[] {
  return read(database.pager, (pages) =>
    [
      ...new TableRows(pages, users).columns([0]).between(first, last, order),
    ].map(([id = null]) => id),
  );
}

describe("IndexRows", () => {
  it("finds the entries equal to a prefix, in key order or its reverse", () => {
    const bob = { equal: ["bob"], range: null };
    expect(rowidsOf(byNick, bob)).toEqual([2, 4]);
    expect(rowidsOf(byNick, bob, "descending")).toEqual([4, 2]);
  });

  it("finds a range with inclusive and exclusive ends", () => {
    expect(nicksIn(within(including("bob"), null))).toEqual([2, 4, 5]);
    expect(nicksIn(within(excluding("bob"), null))).toEqual([5]);
    expect(nicksIn(within(null, including("bob")))).toEqual([1, 2, 4]);
    expect(nicksIn(within(excluding("ann"), excluding("cid")))).toEqual([2, 4]);
  });

  it("never finds a NULL key in a range, however open, both ways; only without a range", () => {
    expect(nicksIn(within(null, excluding("bob")))).toEqual([1]);
    expect(nicksIn(within(null, null))).toEqual([1, 2, 4, 5]);
    const nicksDown = (range: KeyRange) =>
      rowidsOf(byNick, { equal: [], range }, "descending");
    expect(nicksDown(within(null, excluding("bob")))).toEqual([1]);
    expect(nicksDown(within(null, null))).toEqual([5, 4, 2, 1]);
    expect(rowidsOf(byNick, { equal: [], range: null })).toEqual([
      3, 1, 2, 4, 5,
    ]);
  });

  it("finds nothing for a NULL seek value, which `=` and `<` never match", () => {
    expect(rowidsOf(byNick, { equal: [null], range: null })).toEqual([]);
    expect(nicksIn(within(null, including(null)))).toEqual([]);
  });

  it("ranges over the part after an equal prefix, NULLs skipped under an upper bound", () => {
    expect(bobs(null)).toEqual([2, 4]);
    expect(bobs(within(excluding("c"), null))).toEqual([4]);
    expect(bobs(within(null, excluding("z")))).toEqual([4]);
  });

  it("reads the rows of the entries, and calls an entry without a row corrupt", () => {
    const bob = { equal: ["bob"], range: null };
    const rows = read(database.pager, (pages) => {
      const reader = new TableRows(pages, users).columns([2, ROWID_COLUMN]);
      return [...new IndexRows(pages, byNick).rows(bob, "ascending", reader)];
    });
    expect(rows).toEqual([
      [null, 2],
      ["d@x", 4],
    ]);
    write(database.pager, (pages) => {
      new IndexWriter(pages, byNick.rootPage, byNick.name).insert(
        encodeKey(["bob", 9]),
      );
    });
    expect(() =>
      read(database.pager, (pages) => {
        const reader = new TableRows(pages, users).columns([0]);
        return [...new IndexRows(pages, byNick).rows(bob, "ascending", reader)];
      }),
    ).toThrow(CorruptError);
  });

  it("reads MIN and MAX of the part after a prefix from either end, skipping NULLs", () => {
    expect(extreme(byNick, [], "first")).toBe("ann");
    expect(extreme(byNick, [], "last")).toBe("cid");
    expect(extreme(byNickEmail, ["bob"], "first")).toBe("d@x");
    expect(extreme(byNickEmail, ["bob"], "last")).toBe("d@x");
    expect(extreme(byNickEmail, ["zed"], "first")).toBeNull();
    expect(extreme(byNickEmail, [null], "last")).toBeNull();
  });
});

describe("ColumnReader.between", () => {
  it("reads the rows from one rowid to another, both included, an infinite end open", () => {
    expect(between(2, 4)).toEqual([2, 3, 4]);
    expect(between(-Infinity, 2)).toEqual([1, 2]);
    expect(between(4, Infinity)).toEqual([4, 5]);
    expect(between(3, 2)).toEqual([]);
    expect(between(Infinity, Infinity)).toEqual([]);
  });

  it("reads them in reverse, from the last down to the first", () => {
    expect(between(2, 4, "descending")).toEqual([4, 3, 2]);
    expect(between(-Infinity, 2, "descending")).toEqual([2, 1]);
    expect(between(4, Infinity, "descending")).toEqual([5, 4]);
    expect(between(2, 3.5, "descending")).toEqual([3, 2]);
    expect(between(3, 2, "descending")).toEqual([]);
    expect(between(-Infinity, -Infinity, "descending")).toEqual([]);
  });
});
