import { describe, expect, it } from "vitest";
import { checkStorage } from "../check.js";
import { BTreePageEditor } from "../format/btree-page.js";
import { writeUint32 } from "../format/bytes.js";
import { encodeKey } from "../format/key.js";
import { openPager } from "../open.js";
import type { Pager } from "../pager/pager.js";
import { IndexWriter } from "../rows/index-store.js";
import { SchemaTableWriter } from "../rows/schema-table.js";
import { TableReader, TableWriter } from "../rows/table-store.js";
import { memoryFiles, read, write } from "./pager-support.js";

const ROWS = 300;
/** Every fiftieth row has a bio too long for its cell. */
const LONG_BIO = "x".repeat(20_000);
/** The overflow chain's next-page field (docs/file-format.md, Overflow values). */
const NEXT_PAGE_OFFSET = 4;

interface Database {
  readonly pager: Pager;
  readonly tableRoot: number;
  readonly indexRoot: number;
}

/** A table of users with overflowing bios, and an index on their names. */
async function usersDatabase(): Promise<Database> {
  const pager = await openPager(memoryFiles(), {
    name: "check",
    pageSize: 4096,
  });
  const roots = write(pager, (transaction) => {
    const tableRoot = TableWriter.create(transaction);
    const indexRoot = IndexWriter.create(transaction);
    const schema = new SchemaTableWriter(transaction);
    schema.add(entry("table", "users", tableRoot));
    schema.add(entry("index", "users_name", indexRoot));
    const table = new TableWriter(transaction, tableRoot, "users");
    const index = new IndexWriter(transaction, indexRoot, "users_name");
    for (let rowid = 1; rowid <= ROWS; rowid++) {
      const name = `user ${String(rowid)}`;
      table.insert(rowid, [name, rowid % 50 === 0 ? LONG_BIO : "short"]);
      index.insert(encodeKey([name, rowid]));
    }
    return { tableRoot, indexRoot };
  });
  return { pager, ...roots };
}

function entry(type: "table" | "index", name: string, rootPage: number) {
  return { type, name, tableName: "users", rootPage, sql: null };
}

function problemsOf(pager: Pager): string[] {
  return read(pager, (transaction) => checkStorage(transaction));
}

describe("the storage check", () => {
  it("finds nothing wrong in a sound database", async () => {
    const empty = await openPager(memoryFiles(), { name: "empty" });
    expect(problemsOf(empty)).toEqual([]);

    const { pager, tableRoot } = await usersDatabase();
    expect(problemsOf(pager)).toEqual([]);

    write(pager, (transaction) => {
      const table = new TableWriter(transaction, tableRoot, "users");
      for (let rowid = 1; rowid <= ROWS; rowid += 2) table.delete(rowid);
    });
    pager.checkpoint();
    expect(problemsOf(pager)).toEqual([]);
  });

  it("reports a page marked in use that nothing uses", async () => {
    const { pager } = await usersDatabase();
    const leaked = write(pager, (transaction) => transaction.allocate());
    expect(problemsOf(pager)).toEqual([
      `page ${String(leaked)}: is marked in use, but nothing uses it`,
    ]);
  });

  it("reports a page in use that is marked free", async () => {
    const { pager, indexRoot } = await usersDatabase();
    write(pager, (transaction) => {
      transaction.free(indexRoot);
    });
    expect(problemsOf(pager)).toContain(
      `index users_name: page ${String(indexRoot)} is marked free`,
    );
  });

  it("reports a page two trees share", async () => {
    const { pager, tableRoot } = await usersDatabase();
    write(pager, (transaction) => {
      new SchemaTableWriter(transaction).add(entry("table", "copy", tableRoot));
    });
    expect(problemsOf(pager)).toContain(
      `table copy: page ${String(tableRoot)} is used by table users too`,
    );
  });

  it("reports a tree that points back at itself instead of walking it forever", async () => {
    const { pager, tableRoot } = await usersDatabase();
    write(pager, (transaction) => {
      new BTreePageEditor(transaction.write(tableRoot)).setChild(0, tableRoot);
    });
    expect(problemsOf(pager)).toContain(
      `table users: page ${String(tableRoot)}: is reached twice`,
    );
  });

  it("reports an overflow chain cut short", async () => {
    const { pager, tableRoot } = await usersDatabase();
    const chain = read(pager, (transaction) => {
      const row = new TableReader(transaction, tableRoot).get(50);
      return row?.overflowValues()[0];
    });
    if (chain === undefined) throw new Error("row 50 has no overflow chain");
    write(pager, (transaction) => {
      writeUint32(transaction.write(chain.firstPage), NEXT_PAGE_OFFSET, 0);
    });

    const problems = problemsOf(pager);
    expect(problems).toContain(
      `table users: row 50: overflow chain at page ${String(chain.firstPage)} does not hold its ${String(LONG_BIO.length)} bytes`,
    );
    // The pages after the cut are now used by nothing.
    expect(
      problems.some((problem) => problem.includes("nothing uses it")),
    ).toBe(true);
  });
});
