import { describe, expect, it } from "vitest";
import { encodeKey, IndexWriter, TableWriter } from "../../storage/index.js";
import { write } from "../../storage/tests/pager-support.js";
import { usersShape, withUsers } from "./database.js";

describe("checkTables", () => {
  it("finds nothing wrong with rows and their index entries", async () => {
    const { database, users } = await withUsers();
    database.insert(users, [
      [null, "ann", "a@x"],
      [null, "bob", null],
      [null, "cid", null],
    ]);
    expect(database.tableProblems()).toEqual([]);
  });

  it("reports an index entry missing, one no row gives, and a unique key held twice", async () => {
    const { database, users } = await withUsers();
    database.insert(users, [
      [1, "ann", "a@x"],
      [2, "bob", "b@x"],
    ]);
    const [uniqueEmail] = database.indexes;
    write(database.pager, (transaction) => {
      const entries = new IndexWriter(
        transaction,
        uniqueEmail?.rootPage ?? 0,
        "i",
      );
      entries.delete(encodeKey(["b@x", 2]));
      entries.insert(encodeKey(["a@x", 9]));
    });
    expect(database.tableProblems()).toEqual([
      "index opfsql_autoindex_users_1: no entry for row 2",
      "index opfsql_autoindex_users_1: an entry for row 9 that no row gives",
      "index opfsql_autoindex_users_1: unique key held twice, again by row 9",
    ]);
  });

  it("reports a record with more values than its table has columns", async () => {
    const { database, users } = await withUsers();
    write(database.pager, (transaction) => {
      new TableWriter(transaction, users.rootPage, "users").insert(5, [
        null,
        "ann",
        null,
        "a value for no column",
      ]);
    });
    expect(database.tableProblems()).toContain(
      "table users: row 5 has 4 values for 3 columns",
    );
  });

  it("reports an AUTOINCREMENT counter below the largest rowid", async () => {
    const { database, users } = await withUsers({
      ...usersShape,
      isAutoincrement: true,
    });
    database.insert(users, [[null, "ann", null]]);
    write(database.pager, (transaction) => {
      // A row the counter never saw.
      new TableWriter(transaction, users.rootPage, "users").insert(40, [
        null,
        "x",
        null,
      ]);
    });
    expect(database.tableProblems()).toEqual([
      "table users: opfsql_sequence says 1, below rowid 40",
    ]);
  });
});
