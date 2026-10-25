import { describe, expect, it } from "vitest";
import { CorruptError } from "../../errors.js";
import { MemoryFile } from "../file/sync-file.js";
import { openPager } from "../open.js";
import { SCHEMA_ROOT_PAGE } from "../pager/database-file.js";
import type { Pager } from "../pager/pager.js";
import type { WriteTransaction } from "../pager/transaction.js";
import type { SchemaEntry } from "../rows/schema-table.js";
import { readSchemaTable, SchemaTableWriter } from "../rows/schema-table.js";
import { TableWriter } from "../rows/table-store.js";

const users: SchemaEntry = {
  type: "table",
  name: "users",
  tableName: "users",
  rootPage: 3,
  sql: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT UNIQUE)",
};

const usersName: SchemaEntry = {
  type: "index",
  name: "opfsql_autoindex_users_1",
  tableName: "users",
  rootPage: 4,
  sql: null,
};

async function newPager(): Promise<Pager> {
  const files = { database: new MemoryFile(), wal: new MemoryFile() };
  return openPager({ ...files, marks: null }, { name: "test", pageSize: 4096 });
}

function write<T>(pager: Pager, change: (tx: WriteTransaction) => T): T {
  const transaction = pager.beginWrite();
  if (transaction === null) throw new Error("no snapshot");
  const result = change(transaction);
  transaction.commit();
  return result;
}

function readEntries(pager: Pager) {
  const transaction = pager.beginRead();
  if (transaction === null) throw new Error("no snapshot");
  try {
    return readSchemaTable(transaction);
  } finally {
    transaction.end();
  }
}

describe("the schema table", () => {
  it("starts empty and keeps entries in the order they were added", async () => {
    const pager = await newPager();
    expect(readEntries(pager)).toEqual([]);

    const rowids = write(pager, (transaction) => {
      const schema = new SchemaTableWriter(transaction);
      return [schema.add(users), schema.add(usersName)];
    });

    expect(readEntries(pager)).toEqual([
      { rowid: rowids[0], ...users },
      { rowid: rowids[1], ...usersName },
    ]);
  });

  it("moves the schema version forward with every change", async () => {
    const pager = await newPager();
    const versionAfter = (change: (schema: SchemaTableWriter) => void) =>
      write(pager, (transaction) => {
        change(new SchemaTableWriter(transaction));
        return transaction.schemaVersion;
      });

    const added = versionAfter((schema) => schema.add(users));
    const replaced = versionAfter((schema) => {
      schema.replace(1, { ...users, sql: "CREATE TABLE users (id INTEGER)" });
    });
    const removed = versionAfter((schema) => {
      schema.remove(1);
    });

    expect(added).toBeGreaterThan(0);
    expect(replaced).toBeGreaterThan(added);
    expect(removed).toBeGreaterThan(replaced);
  });

  it("refuses rows it cannot make sense of", async () => {
    const pager = await newPager();
    const writeRaw = (values: (string | number | null)[]) => {
      write(pager, (transaction) => {
        const raw = new TableWriter(transaction, SCHEMA_ROOT_PAGE, "raw");
        raw.delete(1);
        raw.insert(1, values);
      });
    };

    writeRaw(["view", "v", "v", 12, null]);
    expect(() => readEntries(pager)).toThrow(CorruptError);

    writeRaw(["table", "t", "t", 1, null]);
    expect(() => readEntries(pager)).toThrow(/column 3/u);
  });
});
