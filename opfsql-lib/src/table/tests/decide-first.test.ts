import { describe, expect, it } from "vitest";
import {
  DatatypeMismatchError,
  IndexKeyTooLargeError,
  RowTooLargeError,
} from "../../errors.js";
import type { IndexShape, Table, TableShape } from "../../schema/index.js";
import type { WriteTransaction } from "../../storage/index.js";
import { write } from "../../storage/tests/pager-support.js";
import type { Value } from "../../values/index.js";
import { castValue } from "../../values/index.js";
import type { CompileExpression, TableEditor } from "../index.js";
import { TableRows } from "../index.js";
import { column, Database, uniqueEmail, usersShape } from "./database.js";

// A refused change must leave the table and every index as they were, even
// inside a transaction that goes on: the change is decided before it is
// written.

/** CAST(nick AS INTEGER), which fails for a nick that is not a number. */
function nickAsInteger(row: readonly Value[]): Value {
  return castValue(row[1] ?? null, "INTEGER");
}

const castNick: CompileExpression = () => nickAsInteger;

const nickNumber: IndexShape = {
  name: "users_nick_number",
  tableName: "users",
  keys: [{ kind: "expression", sql: "CAST(nick AS INTEGER)", type: "INTEGER" }],
  isUnique: false,
  origin: "statement",
};

const byNick: IndexShape = {
  name: "users_nick",
  tableName: "users",
  keys: [{ kind: "column", column: 1 }],
  isUnique: false,
  origin: "statement",
};

async function usersWith(
  index: IndexShape,
  compile?: CompileExpression,
): Promise<{ database: Database; users: Table }> {
  const database = await Database.open(compile);
  const created = database.change((catalog) =>
    catalog.createTable(usersShape, [uniqueEmail]),
  );
  database.tables.push(created.table);
  database.indexes.push(...created.indexes);
  const extra = database.change((catalog) =>
    catalog.createIndex(index, created.table),
  );
  database.indexes.push(extra);
  return { database, users: created.table };
}

/** Runs `change`, expects it refused, and returns what the transaction holds after. */
function refusedWithin(
  database: Database,
  table: Table,
  change: (editor: TableEditor) => unknown,
  error: new (...parameters: never[]) => Error,
): { rows: Value[][]; problems: string[] } {
  return write(database.pager, (transaction: WriteTransaction) => {
    const editor = database.editorIn(transaction, table);
    expect(() => change(editor)).toThrow(error);
    const rows = [...new TableRows(transaction, table).scan()];
    return {
      rows: rows.map((row) => row.values()),
      problems: database.problemsIn(transaction),
    };
  });
}

describe("TableEditor decides a change before writing it", () => {
  it("writes nothing when an expression key cannot be computed", async () => {
    const { database, users } = await usersWith(nickNumber, castNick);
    const after = refusedWithin(
      database,
      users,
      (editor) => editor.insert([null, "abc", "a@x"]),
      DatatypeMismatchError,
    );
    expect(after).toEqual({ rows: [], problems: [] });
  });

  it("writes nothing when a later index refuses the key as too long", async () => {
    const { database, users } = await usersWith(byNick);
    const after = refusedWithin(
      database,
      users,
      (editor) => editor.insert([null, "x".repeat(5000), "a@x"]),
      IndexKeyTooLargeError,
    );
    expect(after).toEqual({ rows: [], problems: [] });
  });

  it("keeps a row whose move to a new rowid is refused as too large", async () => {
    const wide: TableShape = {
      name: "wide",
      columns: Array.from({ length: 300 }, (_, position) =>
        column(`c${String(position)}`, "INTEGER"),
      ),
      rowidColumn: 0,
      isAutoincrement: false,
    };
    const database = await Database.open();
    const { table } = database.change((catalog) =>
      catalog.createTable(wide, []),
    );
    database.tables.push(table);
    const small = wide.columns.map((_, position) => (position === 0 ? 1 : 0));
    database.insert(table, [small]);

    const huge = wide.columns.map((_, position) =>
      position === 0 ? 2 : 2 ** 40 + position,
    );
    const after = refusedWithin(
      database,
      table,
      (editor) => editor.update(1, huge),
      RowTooLargeError,
    );
    expect(after.rows).toEqual([small]);
    expect(after.problems).toEqual([]);
  });

  it("finds the conflict ON CONFLICT names before any other", async () => {
    const { database, users } = await usersWith(byNick);
    database.insert(users, [
      [1, "ann", "a@x"],
      [2, "bob", "b@x"],
    ]);
    const [emailIndex] = database.indexes;
    write(database.pager, (transaction) => {
      const editor = database.editorIn(transaction, users);
      const refused: Value[] = [1, "cid", "b@x"];
      expect(editor.findConflict(refused)).toMatchObject({
        rowid: 1,
        constraint: "rowid",
      });
      expect(editor.findConflict(refused, emailIndex)).toMatchObject({
        rowid: 2,
        constraint: emailIndex,
      });
    });
  });
});
