import { beforeEach, describe, expect, it } from "vitest";
import { MemoryFile } from "../../storage/file/sync-file.js";
import {
  IndexReader,
  IndexWriter,
  openPager,
  readSchemaTable,
  SchemaTableWriter,
  type StorageFiles,
  type WriteTransaction,
} from "../../storage/index.js";
import { createEngine } from "../engine.js";

// Each test builds a healthy database, closes it, damages it the way a bug
// would — through the storage layer, below the table layer that keeps rows
// and indexes together — reopens it and expects checkIntegrity() to name
// the damage. The checks themselves are tested in storage and table; this
// holds the engine to running both, on the latest commit.

let files: StorageFiles;

beforeEach(async () => {
  files = { database: new MemoryFile(), wal: new MemoryFile(), marks: null };
  const engine = await createEngine(files, { pageSize: 4096 });
  const session = engine.connect();
  await session.execute(`
    CREATE TABLE t (id INTEGER PRIMARY KEY, tag INTEGER, body TEXT);
    CREATE INDEX t_tag ON t (tag);
    CREATE TABLE a (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
  `);
  for (let id = 1; id <= 300; id++) {
    await session.query("INSERT INTO t (id, tag, body) VALUES ($1, $2, $3)", [
      id,
      id % 7,
      `row ${String(id)}`.padEnd(40, "."),
    ]);
  }
  await session.execute("DELETE FROM t WHERE id % 5 = 0");
  await session.execute("INSERT INTO a (name) VALUES ('x'), ('y'), ('z')");
  await session.close();
  await engine.close();
});

async function check(): Promise<string[]> {
  const engine = await createEngine(files);
  try {
    return await engine.checkIntegrity();
  } finally {
    await engine.close();
  }
}

/** Changes the closed database in one committed write transaction. */
async function damage(
  edit: (transaction: WriteTransaction) => void,
): Promise<void> {
  const pager = await openPager(files, { name: "damaged" });
  const transaction = pager.beginWrite();
  edit(transaction);
  transaction.commit();
  pager.close();
}

function entryNamed(transaction: WriteTransaction, name: string) {
  const entry = readSchemaTable(transaction).find(
    (candidate) => candidate.name === name,
  );
  if (entry === undefined) throw new Error(`no ${name}`);
  return entry;
}

describe("checkIntegrity", () => {
  it("finds nothing wrong with a healthy database", async () => {
    expect(await check()).toEqual([]);
  });

  it("finds a page marked in use that nothing uses", async () => {
    let leaked = 0;
    await damage((transaction) => {
      leaked = transaction.allocate();
    });
    expect(await check()).toContainEqual(
      expect.stringContaining(`page ${String(leaked)}`),
    );
  });

  it("finds an index entry its row lacks", async () => {
    await damage((transaction) => {
      const { rootPage } = entryNamed(transaction, "t_tag");
      const [first] = new IndexReader(transaction, rootPage).keysFrom();
      new IndexWriter(transaction, rootPage, "t_tag").delete(first);
    });
    expect(await check()).toEqual([
      expect.stringMatching(/^index t_tag: no entry for row \d+$/),
    ]);
  });

  it("reports a schema that does not load, rather than failing", async () => {
    await damage((transaction) => {
      const entry = entryNamed(transaction, "a");
      new SchemaTableWriter(transaction).replace(entry.rowid, {
        ...entry,
        sql: "CREATE TABLE a (",
      });
    });
    expect(await check()).toEqual([
      expect.stringMatching(/^the schema of table a does not load: /),
    ]);
  });
});
