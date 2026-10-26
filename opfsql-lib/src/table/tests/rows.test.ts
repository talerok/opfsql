import { describe, expect, it } from "vitest";
import { ROWID_COLUMN } from "../../schema/index.js";
import { read } from "../../storage/tests/pager-support.js";
import type { Value } from "../../values/index.js";
import { TableRows } from "../index.js";
import { column, withUsers } from "./database.js";

// users (id INTEGER PRIMARY KEY, nick TEXT, email TEXT UNIQUE)

async function threeUsers() {
  const { database, users } = await withUsers();
  database.insert(users, [
    [1, "ann", "a@x"],
    [5, "bob", null],
    [9, "cid", "c@x"],
  ]);
  return { database, users };
}

describe("ColumnReader", () => {
  it("reads chosen columns in a chosen order, the rowid column and later columns' defaults filled", async () => {
    const { database, users } = await threeUsers();
    // A column added after the rows were written reads its default.
    const widened = {
      ...users,
      columns: [
        ...users.columns,
        column("age", "INTEGER", { defaultValue: 0 }),
      ],
    };
    const read3 = (columns: number[], order: "ascending" | "descending") =>
      read(database.pager, (pages) => [
        ...new TableRows(pages, widened)
          .columns(columns)
          .between(-Infinity, Infinity, order),
      ]);
    expect(read3([3, 0, 1], "ascending")).toEqual([
      [0, 1, "ann"],
      [0, 5, "bob"],
      [0, 9, "cid"],
    ]);
    expect(read3([ROWID_COLUMN, 2], "descending")).toEqual([
      [9, "c@x"],
      [5, null],
      [1, "a@x"],
    ]);
  });

  it("reads one row by rowid, or none", async () => {
    const { database, users } = await threeUsers();
    const at = (rowid: number): Value[] | null =>
      read(database.pager, (pages) =>
        new TableRows(pages, users).columns([1, 0]).at(rowid),
      );
    expect(at(5)).toEqual(["bob", 5]);
    expect(at(6)).toBeNull();
  });
});
