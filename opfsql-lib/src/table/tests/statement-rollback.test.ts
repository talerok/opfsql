import { describe, expect, it } from "vitest";
import { ConstraintError } from "../../errors.js";
import { write } from "../../storage/tests/pager-support.js";
import { TableRows } from "../index.js";
import { withUsers } from "./database.js";

// A statement that fails part way rolls back to the savepoint taken before
// it, and the transaction goes on, as SQLite does.

describe("a statement rolled back to its savepoint", () => {
  it("leaves the rows and indexes from before it, and the transaction goes on", async () => {
    const { database, users } = await withUsers();
    const rowids = write(database.pager, (transaction) => {
      const editor = database.editorIn(transaction, users);
      editor.insert([null, "ann", "a@x"]);

      const statement = transaction.savepoint();
      expect(() => {
        for (let count = 0; count < 40; count++) {
          editor.insert([null, `n${String(count)}`, `${String(count)}@x`]);
        }
        editor.insert([null, "dup", "a@x"]);
      }).toThrow(ConstraintError);
      transaction.rollbackTo(statement);
      transaction.release(statement);
      expect(database.problemsIn(transaction)).toEqual([]);

      // The next statement sees the table as the failed one found it.
      const next = database.editorIn(transaction, users);
      next.insert([null, "bob", "b@x"]);
      return [...new TableRows(transaction, users).scan()].map(
        (row) => row.rowid,
      );
    });
    expect(rowids).toEqual([1, 2]);
    expect(database.rows(users)).toEqual([
      [1, "ann", "a@x"],
      [2, "bob", "b@x"],
    ]);
    expect(database.tableProblems()).toEqual([]);
  });
});
