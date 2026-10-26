import { describe, expect, it } from "vitest";
import { InternalError } from "../../errors.js";
import type { IndexShape } from "../../schema/index.js";
import { read, write } from "../../storage/tests/pager-support.js";
import type { Value } from "../../values/index.js";
import {
  type CompileExpression,
  IndexKeys,
  Tables,
  WritableTables,
} from "../index.js";
import { Database, usersShape } from "./database.js";

// users (id INTEGER PRIMARY KEY, nick TEXT, email TEXT UNIQUE), here with an
// index on lower(nick) whose compilations are counted.

const lowerNick: IndexShape = {
  name: "users_lower_nick",
  table: "users",
  keys: [{ kind: "expression", sql: "lower(nick)", type: "TEXT" }],
  isUnique: false,
  origin: "statement",
};

function lowered(row: readonly Value[]): Value {
  const nick = row[1];
  return typeof nick === "string" ? nick.toLowerCase() : null;
}

/** A compiler of lower(nick) that counts its compilations. */
function countingCompiler(): {
  compile: CompileExpression;
  count: () => number;
} {
  let compilations = 0;
  const compile: CompileExpression = () => {
    compilations++;
    return lowered;
  };
  return { compile, count: () => compilations };
}

async function usersDatabase(isAutoincrement = false): Promise<Database> {
  const { compile } = countingCompiler();
  const database = await Database.open(compile);
  const shape = { ...usersShape, isAutoincrement };
  const { table } = database.change((catalog) =>
    catalog.createTable(shape, []),
  );
  database.tables.push(table);
  database.findSequences();
  const index = database.change((catalog) =>
    catalog.createIndex(lowerNick, table),
  );
  database.indexes.push(index);
  return database;
}

describe("Tables", () => {
  it("edit a table with every index kept, each key compiled once per IndexKeys", async () => {
    const database = await usersDatabase();
    const { compile, count } = countingCompiler();
    const keys = new IndexKeys(database.schema, compile);
    const users = database.schema.table("users");
    if (users === null) throw new Error("no users table");
    for (const nick of ["Ann", "BOB"]) {
      write(database.pager, (pages) => {
        new WritableTables(pages, keys)
          .editor(users)
          .insert([null, nick, null]);
      });
    }
    expect(count()).toBe(1);
    expect(database.tableProblems()).toEqual([]);
    const index = database.schema.index("users_lower_nick");
    if (index === null) throw new Error("no index");
    const found = read(database.pager, (pages) => {
      const seek = { equal: ["bob"], range: null };
      return [
        ...new Tables(pages, database.schema)
          .index(index)
          .rowids(seek, "ascending"),
      ];
    });
    expect(found).toEqual([2]);
  });

  it("give an AUTOINCREMENT table's editor its counter, and refuse one without it", async () => {
    const database = await usersDatabase(true);
    const users = database.schema.table("users");
    if (users === null) throw new Error("no users table");
    const keys = new IndexKeys(database.schema, countingCompiler().compile);
    write(database.pager, (pages) => {
      const editor = new WritableTables(pages, keys).editor(users);
      expect(editor.insert([null, "a", null])).toBe(1);
      editor.delete(1);
      expect(editor.insert([null, "b", null])).toBe(2);
    });
    // The schema without opfsql_sequence, which findSequences added last.
    database.tables.pop();
    const withoutCounter = new IndexKeys(
      database.schema,
      countingCompiler().compile,
    );
    write(database.pager, (pages) => {
      const tables = new WritableTables(pages, withoutCounter);
      expect(() => tables.editor(users)).toThrow(InternalError);
    });
  });

  it("report the row an insert refused as it would have been stored", async () => {
    const database = await usersDatabase();
    const users = database.schema.table("users");
    if (users === null) throw new Error("no users table");
    const keys = new IndexKeys(database.schema, countingCompiler().compile);
    write(database.pager, (pages) => {
      const editor = new WritableTables(pages, keys).editor(users);
      editor.insert([7, "Ann", null]);
      const conflict = editor.findConflict(["7", 1, null]);
      expect(conflict?.refused).toEqual([7, "1", null]);
    });
  });
});
