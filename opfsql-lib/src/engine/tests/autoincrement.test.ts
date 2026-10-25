import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Engine } from "../engine.js";
import type { Session } from "../session.js";
import { openTestEngine } from "./test-engine.js";

// The AUTOINCREMENT sequence lives with the table's data, not in the catalog,
// so inserts never change the schema.

let engine: Engine;
let session: Session;
let dbName: string;
let seq = 0;

beforeEach(async () => {
  resetMockOPFS();
  dbName = `autoincrement-${seq++}`;
  engine = await openTestEngine(dbName);
  session = engine.connect();
  await session.execute(
    "CREATE TABLE a (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
  );
  await session.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, name TEXT)");
});

afterEach(async () => {
  await session.close();
  await engine.close();
});

async function ids(table: string): Promise<unknown[]> {
  const [r] = await session.execute(`SELECT id FROM ${table} ORDER BY id`);
  return r.objects().map((row) => row.id);
}

describe("AUTOINCREMENT", () => {
  it("leaves prepared statements valid, including on other tables", async () => {
    const insert = await session.prepare("INSERT INTO a (name) VALUES ($1)");
    const select = await session.prepare("SELECT name FROM b WHERE id = $1");

    for (const name of ["x", "y", "z"]) await insert.query([name]);
    const selected = await select.query([1]);
    expect(selected.rows).toEqual([]);
    expect(await ids("a")).toEqual([1, 2, 3]);
  });

  it("does not change the schema", async () => {
    const before = await session.schema();
    await session.execute("INSERT INTO a (name) VALUES ('x')");
    await session.execute("INSERT INTO a (id, name) VALUES (10, 'y')");
    expect(await session.schema()).toEqual(before);
  });

  it("keeps the sequence across reopening the database", async () => {
    await session.execute("INSERT INTO a (name) VALUES ('x')");
    await session.execute("INSERT INTO a (name) VALUES ('y')");
    await session.execute("DELETE FROM a WHERE id = 2");
    await session.close();
    await engine.close();

    engine = await openTestEngine(dbName);
    session = engine.connect();
    await session.execute("INSERT INTO a (name) VALUES ('z')");
    expect(await ids("a")).toEqual([1, 3]);
  });
});
