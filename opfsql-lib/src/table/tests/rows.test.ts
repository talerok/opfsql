import { describe, expect, it } from "vitest";
import { TableRows } from "../index.js";
import { column, withUsers } from "./database.js";
import { read } from "../../storage/tests/pager-support.js";

describe("TableRows", () => {
  it("scans backward as its table sees the rows: rowid column and defaults filled", async () => {
    const { database, users } = await withUsers();
    database.insert(users, [
      [1, "ann", "a@x"],
      [5, "bob", null],
      [9, "cid", "c@x"],
    ]);
    // A column added after the rows were written reads its default.
    const widened = {
      ...users,
      columns: [
        ...users.columns,
        column("age", "INTEGER", { defaultValue: 0 }),
      ],
    };
    const backward = read(database.pager, (transaction) =>
      [...new TableRows(transaction, widened).scanBackward()].map((row) =>
        row.values(),
      ),
    );
    expect(backward).toEqual([
      [9, "cid", "c@x", 0],
      [5, "bob", null, 0],
      [1, "ann", "a@x", 0],
    ]);
    const below = read(database.pager, (transaction) =>
      [...new TableRows(transaction, users).scanBackward(5)].map(
        (row) => row.rowid,
      ),
    );
    expect(below).toEqual([1]);
  });
});
