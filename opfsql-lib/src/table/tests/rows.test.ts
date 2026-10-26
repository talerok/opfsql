import { describe, expect, it } from "vitest";
import { type Column, ROWID_COLUMN } from "../../schema/index.js";
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

/**
 * Every read of `added`, a column added after three users were written, so
 * they read its default: twice per tuple by columns, by rowid, and row by
 * row — eleven values — and a way to read them all again.
 */
async function everyReadOfAdded(
  added: Column,
): Promise<{ reads: Value[]; again: () => Value[] }> {
  const { database, users } = await threeUsers();
  const widened = { ...users, columns: [...users.columns, added] };
  const again = (): Value[] =>
    read(database.pager, (pages) => {
      const rows = new TableRows(pages, widened);
      const twice = rows.columns([3, 3]);
      return [
        ...[...twice.between(-Infinity, Infinity)].flat(),
        ...(twice.at(5) ?? []),
        ...[...rows.scan()].map((row) => row.value(3)),
      ];
    });
  return { reads: again(), again };
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

  it("reads a JSON default as each row's own copy, by columns or by rows", async () => {
    const fallback = ["new"];
    const added = column("tags", "JSON", { defaultValue: fallback });
    const { reads, again } = await everyReadOfAdded(added);
    expect(reads).toEqual(Array.from({ length: 11 }, () => ["new"]));
    // Every one a different array, and none the column's default itself.
    expect(new Set([...reads, fallback]).size).toBe(reads.length + 1);
    for (const value of reads) if (Array.isArray(value)) value.push("changed");
    expect(again()).toEqual(reads.map(() => ["new"]));
    expect(fallback).toEqual(["new"]);
  });

  it("reads a BLOB default as each row's own copy, by columns or by rows", async () => {
    const fallback = new Uint8Array([1, 2]);
    const added = column("data", "BLOB", { defaultValue: fallback });
    const { reads, again } = await everyReadOfAdded(added);
    expect(reads).toEqual(Array.from({ length: 11 }, () => fallback));
    expect(new Set([...reads, fallback]).size).toBe(reads.length + 1);
    for (const value of reads) if (value instanceof Uint8Array) value.fill(0);
    expect(again()).toEqual(reads.map(() => new Uint8Array([1, 2])));
    expect(fallback).toEqual(new Uint8Array([1, 2]));
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
