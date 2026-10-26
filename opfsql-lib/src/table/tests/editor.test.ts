import { describe, expect, it } from "vitest";
import { column } from "../../../test/support/schema.js";
import { ConstraintError, DatatypeMismatchError } from "../../errors.js";
import type { Index, Table } from "../../schema/index.js";
import { Schema } from "../../schema/index.js";
import type { Pager, WriteTransaction } from "../../storage/index.js";
import { IndexWriter, openPager, TableWriter } from "../../storage/index.js";
import { memoryFiles, read, write } from "../../storage/tests/pager-support.js";
import type { Value } from "../../values/index.js";
import type { CompileExpression } from "../index.js";
import { checkTables, keyedIndex, Sequences, TableEditor } from "../index.js";

// users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT NOT NULL,
//        age INTEGER DEFAULT 0), with an index on lower(name).
const ID = 0;
const EMAIL = 1;

/** lower(name), as the engine would compile it. */
function lowerName(row: readonly Value[]): Value {
  const name = row[2];
  return typeof name === "string" ? name.toLowerCase() : null;
}

const compile: CompileExpression = () => lowerName;

interface Users {
  readonly pager: Pager;
  readonly table: Table;
  readonly indexes: Index[];
  readonly sequencesRoot: number;
}

async function usersTable(isAutoincrement = false): Promise<Users> {
  const pager = await openPager(memoryFiles(), { name: "users" });
  return write(pager, (transaction) => {
    const table: Table = {
      name: "users",
      columns: [
        column("id", "INTEGER"),
        column("email", "TEXT"),
        column("name", "TEXT", { isNotNull: true }),
        column("age", "INTEGER", { defaultValue: 0 }),
      ],
      rootPage: TableWriter.create(transaction),
      rowidColumn: ID,
      isAutoincrement,
    };
    const indexes: Index[] = [
      {
        name: "opfsql_autoindex_users_1",
        table: "users",
        keys: [{ kind: "column", column: EMAIL }],
        isUnique: true,
        rootPage: IndexWriter.create(transaction),
        origin: "unique",
      },
      {
        name: "users_lower_name",
        table: "users",
        keys: [{ kind: "expression", sql: "lower(name)", type: "TEXT" }],
        isUnique: false,
        rootPage: IndexWriter.create(transaction),
        origin: "statement",
      },
    ];
    const sequencesRoot = Sequences.create(transaction);
    return { pager, table, indexes, sequencesRoot };
  });
}

function editorIn(transaction: WriteTransaction, users: Users): TableEditor {
  const { table, indexes, sequencesRoot } = users;
  const keyed = indexes.map((index) => keyedIndex(index, table, compile));
  const sequences = new Sequences(transaction, sequencesRoot);
  return new TableEditor(transaction, table, keyed, sequences);
}

/** Runs `change` on the users table in a transaction. */
function edit<T>(users: Users, change: (editor: TableEditor) => T): T {
  return write(users.pager, (transaction) =>
    change(editorIn(transaction, users)),
  );
}

function rowsOf(users: Users): Value[][] {
  return edit(users, (editor) => [...editor.scan()].map((row) => row.values()));
}

/** The table and its indexes agree: checkTables finds nothing wrong. */
function expectIndexesMatchRows(users: Users): void {
  const schema = new Schema(0, [users.table], users.indexes);
  const problems = read(users.pager, (transaction) =>
    checkTables(transaction, schema, compile),
  );
  expect(problems).toEqual([]);
}

describe("TableEditor", () => {
  it("assigns rowids, converts values and fills the rowid column", async () => {
    const users = await usersTable();
    edit(users, (editor) => {
      expect(editor.insert([null, "a@x", "Ann", "42"])).toBe(1);
      expect(editor.insert([10, null, "Bob", 0])).toBe(10);
      expect(editor.insert([null, null, "Cid", 7])).toBe(11);
    });
    expect(rowsOf(users)).toEqual([
      [1, "a@x", "Ann", 42],
      [10, null, "Bob", 0],
      [11, null, "Cid", 7],
    ]);
    expectIndexesMatchRows(users);
  });

  it("refuses NULL in a NOT NULL column and a value its type cannot hold", async () => {
    const users = await usersTable();
    edit(users, (editor) => {
      expect(() => editor.insert([null, "a@x", null, 1])).toThrow(
        new ConstraintError("not-null", "users", ["name"]),
      );
      expect(() => editor.insert([null, "a@x", "Ann", "old"])).toThrow(
        DatatypeMismatchError,
      );
      expect(() => editor.insert([null, "a@x", "Ann", "old"])).toThrow(
        /^users\.age: cannot store "old" as INTEGER$/u,
      );
    });
    expect(rowsOf(users)).toEqual([]);
  });

  it("refuses a taken rowid or unique key, writing nothing", async () => {
    const users = await usersTable();
    edit(users, (editor) => editor.insert([1, "a@x", "Ann", 1]));

    edit(users, (editor) => {
      expect(() => editor.insert([1, "b@x", "Bob", 2])).toThrow(
        "UNIQUE constraint failed: users.id",
      );
      expect(() => editor.insert([2, "a@x", "Bob", 2])).toThrow(
        "UNIQUE constraint failed: users.email",
      );
      // NULLs are distinct: any number of rows may have no email.
      editor.insert([null, null, "Cid", 3]);
      editor.insert([null, null, "Dan", 4]);
    });
    expect(rowsOf(users).map((row) => row[ID])).toEqual([1, 2, 3]);
    expectIndexesMatchRows(users);
  });

  it("finds the row an insert would conflict with", async () => {
    const users = await usersTable();
    edit(users, (editor) => {
      editor.insert([5, "a@x", "Ann", 1]);
      expect(editor.findConflict([null, "a@x", "Bob", 2])?.rowid).toBe(5);
      expect(editor.findConflict([5, "b@x", "Bob", 2])?.rowid).toBe(5);
      expect(editor.findConflict([null, "b@x", "Bob", 2])).toBeNull();
    });
  });

  it("updates rows and their index entries, rowid included", async () => {
    const users = await usersTable();
    edit(users, (editor) => {
      editor.insert([1, "a@x", "Ann", 1]);
      editor.insert([2, "b@x", "Bob", 2]);
      // Keeping its own unique key is no conflict.
      expect(editor.update(1, [1, "a@x", "ANN", 1])).toBe(1);
      expect(editor.update(2, [20, "c@x", "Bob", 3])).toBe(20);
      expect(() => editor.update(20, [20, "a@x", "Bob", 3])).toThrow(
        ConstraintError,
      );
      expect(() => editor.update(20, [1, "c@x", "Bob", 3])).toThrow(
        "UNIQUE constraint failed: users.id",
      );
      expect(() => editor.update(20, [null, "c@x", "Bob", 3])).toThrow(
        "NOT NULL constraint failed: users.id",
      );
    });
    expect(rowsOf(users)).toEqual([
      [1, "a@x", "ANN", 1],
      [20, "c@x", "Bob", 3],
    ]);
    expectIndexesMatchRows(users);
  });

  it("deletes rows with their index entries", async () => {
    const users = await usersTable();
    edit(users, (editor) => {
      editor.insert([1, "a@x", "Ann", 1]);
      expect(editor.delete(1)).toBe(true);
      expect(editor.delete(1)).toBe(false);
      editor.insert([2, "a@x", "Bob", 2]);
    });
    expect(rowsOf(users)).toEqual([[2, "a@x", "Bob", 2]]);
    expectIndexesMatchRows(users);
  });

  it.each([
    ["with AUTOINCREMENT, never reuses a rowid", true, 4],
    ["without, reuses the largest one's next", false, 3],
  ])("%s", async (_, isAutoincrement, expected) => {
    const users = await usersTable(isAutoincrement);
    edit(users, (editor) => {
      for (const name of ["Ann", "Bob", "Cid"]) {
        editor.insert([null, null, name, 0]);
      }
      editor.delete(3);
    });
    const next = edit(users, (editor) => editor.insert([null, null, "Dan", 0]));
    expect(next).toBe(expected);
  });

  it("reads a column added after a row was written as its default", async () => {
    const users = await usersTable();
    write(users.pager, (transaction) => {
      // A row written before `age` existed: three values, the rowid's NULL.
      new TableWriter(transaction, users.table.rootPage, "users").insert(7, [
        null,
        "a@x",
        "Ann",
      ]);
    });
    expect(rowsOf(users)).toEqual([[7, "a@x", "Ann", 0]]);
  });
});
